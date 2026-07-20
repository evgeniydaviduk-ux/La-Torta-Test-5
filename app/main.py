import asyncio
import json
import logging
import os
import re
from contextlib import suppress
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiosqlite
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession
from dotenv import load_dotenv
from openai import AsyncOpenAI
from rapidfuzz import fuzz

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("latorta")

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
OPENAI_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4.1-mini").strip()
MANAGER_USERNAME = os.getenv("MANAGER_USERNAME", "byviktoriia_a").strip().lstrip("@")
MANAGER_RAW = os.getenv("MANAGER_CHAT_ID", "").strip()
MANAGER_CHAT_ID = int(MANAGER_RAW) if MANAGER_RAW.lstrip("-").isdigit() else None

SITE_URL = os.getenv("SITE_URL", "https://la-torta.ua/ua/").strip()
SITE_INDEX_URL = "https://la-torta.ua/ua/siteindex/"
MAX_CATEGORY_PAGES = max(10, int(os.getenv("MAX_CATEGORY_PAGES", "350")))
MAX_PRODUCTS = max(50, int(os.getenv("MAX_PRODUCTS", "2500")))
REFRESH_HOURS = max(1, int(os.getenv("CATALOG_REFRESH_HOURS", "12")))
START_CATALOG_REFRESH = os.getenv("START_CATALOG_REFRESH", "true").lower() in {"1","true","yes","on"}
PORT = int(os.getenv("PORT", "10000"))

DB = Path("data/bot.sqlite3")
KNOWLEDGE = Path("data/knowledge.md")

router = Router()
openai_client = AsyncOpenAI(api_key=OPENAI_KEY) if OPENAI_KEY else None

catalog_state = {
    "running": False,
    "last_count": 0,
    "last_error": None,
    "last_http_status": None,
    "categories_found": 0,
}

MAIN_KB = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="🔎 Знайти товар"), KeyboardButton(text="🚚 Доставка й оплата")],
        [KeyboardButton(text="📍 Магазини"), KeyboardButton(text="👩‍💼 Покликати менеджера")],
    ],
    resize_keyboard=True,
    input_field_placeholder="Напишіть, що ви шукаєте…",
)

WELCOME = """Вітаємо у La-Torta! 🎂

Я — AI-помічник магазину. Допоможу знайти товар, підібрати інгредієнти,
форми, барвники, декор і пакування, зорієнтую за ціною та передам
складне питання менеджеру.

Напишіть, що саме ви шукаєте."""

def clean(value):
    return re.sub(r"\s+", " ", value or "").strip()

def valid_site_url(url):
    parsed = urlparse(url)
    return (
        parsed.scheme in {"http", "https"}
        and parsed.netloc.endswith("la-torta.ua")
        and "/ua/" in parsed.path
    )

async def init_db():
    DB.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB) as db:
        await db.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS products(
            url TEXT PRIMARY KEY,
            title TEXT NOT NULL,
            price TEXT,
            stock TEXT,
            image TEXT,
            description TEXT,
            search_text TEXT NOT NULL,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS messages(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id INTEGER NOT NULL,
            role TEXT NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS events(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            type TEXT NOT NULL,
            chat_id INTEGER,
            payload TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        """)
        await db.commit()

async def clear_products():
    async with aiosqlite.connect(DB) as db:
        await db.execute("DELETE FROM products")
        await db.commit()

async def upsert_product(product):
    async with aiosqlite.connect(DB) as db:
        await db.execute("""
        INSERT INTO products(url,title,price,stock,image,description,search_text)
        VALUES(?,?,?,?,?,?,?)
        ON CONFLICT(url) DO UPDATE SET
            title=excluded.title,
            price=excluded.price,
            stock=excluded.stock,
            image=excluded.image,
            description=excluded.description,
            search_text=excluded.search_text,
            updated_at=CURRENT_TIMESTAMP
        """, (
            product["url"], product["title"], product.get("price",""),
            product.get("stock",""), product.get("image",""),
            product.get("description",""), product["search_text"],
        ))
        await db.commit()

async def product_count():
    async with aiosqlite.connect(DB) as db:
        cur = await db.execute("SELECT COUNT(*) FROM products")
        row = await cur.fetchone()
        return int(row[0])

async def all_products():
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute("SELECT * FROM products")
        return [dict(row) for row in await cur.fetchall()]

async def save_message(chat_id, role, text):
    async with aiosqlite.connect(DB) as db:
        await db.execute(
            "INSERT INTO messages(chat_id,role,text) VALUES(?,?,?)",
            (chat_id, role, text[:10000]),
        )
        await db.commit()

async def recent_messages(chat_id, limit=6):
    async with aiosqlite.connect(DB) as db:
        db.row_factory = aiosqlite.Row
        cur = await db.execute(
            "SELECT role,text FROM messages WHERE chat_id=? ORDER BY id DESC LIMIT ?",
            (chat_id, limit),
        )
        return [dict(row) for row in reversed(await cur.fetchall())]

async def browser_get(session, url, timeout=35):
    try:
        response = await session.get(
            url,
            impersonate="chrome",
            timeout=timeout,
            allow_redirects=True,
            headers={
                "Accept-Language": "uk-UA,uk;q=0.9,ru;q=0.8,en;q=0.7",
                "Referer": "https://www.google.com/",
                "Cache-Control": "no-cache",
            },
        )
        catalog_state["last_http_status"] = response.status_code
        if response.status_code != 200:
            log.warning("HTTP %s for %s", response.status_code, url)
            return None
        return response.text
    except Exception as exc:
        catalog_state["last_error"] = f"{type(exc).__name__}: {exc}"
        log.warning("Browser fetch failed %s: %s", url, exc)
        return None

def extract_category_urls(html):
    soup = BeautifulSoup(html, "lxml")
    urls = set()
    excluded = (
        "/contacts", "/shipping", "/payment", "/about", "/reviews",
        "/blog", "/login", "/register", "/cart", "/wishlist", "/compare",
        "/siteindex",
    )
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(SITE_INDEX_URL, href).split("#")[0]
        if not valid_site_url(url):
            continue
        if any(part in url.lower() for part in excluded):
            continue
        text = clean(a.get_text(" ", strip=True))
        if text and len(text) <= 120:
            urls.add(url)
    return list(urls)

def find_card_root(anchor):
    current = anchor
    for _ in range(7):
        if current is None:
            break
        classes = " ".join(current.get("class", []))
        text = clean(current.get_text(" ", strip=True))
        if (
            re.search(r"\b\d[\d\s.,]*\s*(грн|₴)\b", text, flags=re.I)
            or any(token in classes.lower() for token in ("catalog", "product", "item"))
        ):
            return current
        current = current.parent
    return anchor.parent

def parse_category_products(page_url, html):
    soup = BeautifulSoup(html, "lxml")
    products = {}
    anchors = soup.select("a[href]")

    for anchor in anchors:
        href = anchor.get("href")
        title = clean(anchor.get_text(" ", strip=True))
        if not href or len(title) < 5 or len(title) > 220:
            continue

        url = urljoin(page_url, href).split("#")[0]
        if not valid_site_url(url) or url.rstrip("/") == page_url.rstrip("/"):
            continue

        root = find_card_root(anchor)
        root_text = clean(root.get_text(" ", strip=True)) if root else ""
        price_match = re.search(
            r"(\d[\d\s.,]{0,12})\s*(грн|₴)",
            root_text,
            flags=re.I,
        )
        article_signal = bool(re.search(r"Артикул\s*:", root_text, flags=re.I))
        buy_signal = any(word in root_text.lower() for word in ("купити", "в кошик", "новинка"))

        # La-Torta category cards visibly contain a price and/or article.
        if not price_match and not article_signal and not buy_signal:
            continue

        # Reject navigation/category titles.
        if title.lower() in {
            "головна", "каталог", "інгредієнти кондитера",
            "харчові барвники", "форми для випікання", "інвентар",
        }:
            continue

        price = ""
        if price_match:
            price = clean(f"{price_match.group(1)} грн")

        image = ""
        image_node = root.select_one("img") if root else None
        if image_node:
            image = (
                image_node.get("src")
                or image_node.get("data-src")
                or image_node.get("data-lazy")
                or ""
            )
            image = urljoin(page_url, image) if image else ""

        stock = ""
        low = root_text.lower()
        if "немає в наявності" in low or "нет в наличии" in low:
            stock = "Немає в наявності"
        elif "в наявності" in low or "в наличии" in low or price:
            stock = "Уточнюйте на сайті"

        product = {
            "url": url,
            "title": title,
            "price": price,
            "stock": stock,
            "image": image,
            "description": "",
            "search_text": clean(f"{title} {root_text} {price}").lower(),
        }
        products[url] = product

    return list(products.values())

def pagination_urls(page_url, html):
    soup = BeautifulSoup(html, "lxml")
    urls = set()
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        url = urljoin(page_url, href).split("#")[0]
        parsed = urlparse(url)
        if not valid_site_url(url):
            continue
        query = parsed.query.lower()
        text = clean(a.get_text(" ", strip=True)).lower()
        if (
            "page=" in query
            or re.fullmatch(r"\d{1,3}", text)
            or text in {"наступна", "далі", "next", "›", "»"}
        ):
            urls.add(url)
    return urls

async def refresh_catalog():
    if catalog_state["running"]:
        return catalog_state["last_count"]

    catalog_state["running"] = True
    catalog_state["last_error"] = None
    catalog_state["categories_found"] = 0

    try:
        async with AsyncSession() as session:
            index_html = await browser_get(session, SITE_INDEX_URL)
            if not index_html:
                raise RuntimeError(
                    f"Не удалось открыть {SITE_INDEX_URL}; HTTP={catalog_state['last_http_status']}"
                )

            category_urls = extract_category_urls(index_html)
            catalog_state["categories_found"] = len(category_urls)
            log.info("Category URLs discovered: %s", len(category_urls))

            if not category_urls:
                raise RuntimeError("Site index opened, but category links were not parsed")

            await clear_products()

            queue = list(category_urls[:MAX_CATEGORY_PAGES])
            visited = set()
            saved_urls = set()

            while queue and len(visited) < MAX_CATEGORY_PAGES and len(saved_urls) < MAX_PRODUCTS:
                page_url = queue.pop(0)
                if page_url in visited:
                    continue
                visited.add(page_url)

                html = await browser_get(session, page_url)
                if not html:
                    continue

                products = parse_category_products(page_url, html)
                for product in products:
                    if product["url"] in saved_urls:
                        continue
                    await upsert_product(product)
                    saved_urls.add(product["url"])
                    if len(saved_urls) >= MAX_PRODUCTS:
                        break

                for next_url in pagination_urls(page_url, html):
                    if next_url not in visited and len(queue) < MAX_CATEGORY_PAGES * 2:
                        queue.append(next_url)

                if len(visited) % 10 == 0:
                    log.info(
                        "Catalog: pages=%s products=%s queue=%s",
                        len(visited), len(saved_urls), len(queue)
                    )

            catalog_state["last_count"] = len(saved_urls)
            log.info(
                "Catalog refresh complete: categories=%s pages=%s products=%s",
                len(category_urls), len(visited), len(saved_urls)
            )
            return len(saved_urls)

    except Exception as exc:
        catalog_state["last_error"] = f"{type(exc).__name__}: {exc}"
        log.exception("Catalog refresh failed")
        return 0
    finally:
        catalog_state["running"] = False

async def search_products(query, limit=4):
    products = await all_products()
    normalized = clean(query).lower()
    words = [
        w for w in re.findall(r"[\w-]+", normalized, flags=re.UNICODE)
        if len(w) > 1
    ]
    scored = []

    for product in products:
        haystack = product["search_text"]
        exact_hits = sum(word in haystack for word in words)
        title_score = fuzz.token_set_ratio(normalized, product["title"].lower())
        score = exact_hits * 45 + title_score
        if exact_hits or title_score >= 42:
            scored.append((score, product))

    scored.sort(key=lambda item: item[0], reverse=True)
    return [p for _, p in scored[:limit]]

async def ai_answer(chat_id, text, products):
    if not openai_client:
        if products:
            return "Я знайшла кілька товарів і покажу перевірені картки нижче."
        return "Поки не знайшла точний товар. Уточніть назву, бренд, колір або розмір."

    knowledge = KNOWLEDGE.read_text("utf-8") if KNOWLEDGE.exists() else ""
    catalog = "\n\n".join(
        f"{i+1}. {p['title']}\n"
        f"Ціна: {p['price'] or 'не вказана'}\n"
        f"Статус: {p['stock'] or 'перевірити на сайті'}"
        for i,p in enumerate(products)
    ) or "Релевантних товарів не знайдено."

    history = await recent_messages(chat_id)
    messages = [{
        "role": "developer",
        "content": f"""Ти AI-консультант магазину La-Torta.
Відповідай мовою клієнта: українською або російською.
Не вигадуй товари, ціни, наявність, характеристики або посилання.
ЗАБОРОНЕНО писати URL. Перевірені картки бот надішле окремо.
Якщо товарів немає в результатах пошуку, чесно скажи, що точний товар не знайдений.

БАЗА ЗНАНЬ:
{knowledge}

ЗНАЙДЕНІ ТОВАРИ:
{catalog}"""
    }]
    for item in history:
        messages.append({
            "role": "assistant" if item["role"] == "assistant" else "user",
            "content": item["text"],
        })
    messages.append({"role": "user", "content": text})

    response = await openai_client.responses.create(
        model=OPENAI_MODEL,
        input=messages,
        max_output_tokens=350,
    )
    answer = response.output_text.strip()
    answer = re.sub(r"https?://\S+|www\.\S+|la-torta\.ua\S*", "", answer, flags=re.I)
    return clean(answer)

def product_keyboard(url):
    return InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Переглянути товар", url=url)]]
    )

def manager_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(
                text="Написати Вікторії",
                url=f"https://t.me/{MANAGER_USERNAME}",
            )
        ]]
    )

@router.message(CommandStart())
async def start_handler(message: Message):
    await message.answer(WELCOME, reply_markup=MAIN_KB)

@router.message(Command("myid"))
async def myid_handler(message: Message):
    await message.answer(f"Ваш Telegram chat ID: <code>{message.chat.id}</code>")

@router.message(Command("status"))
async def status_handler(message: Message):
    count = await product_count()
    await message.answer(
        "Статус бота ✅\n"
        f"Товарів у базі: {count}\n"
        f"Категорій знайдено: {catalog_state['categories_found']}\n"
        f"Оновлення: {'виконується' if catalog_state['running'] else 'не виконується'}\n"
        f"Останній HTTP: {catalog_state['last_http_status']}\n"
        f"Остання помилка: {catalog_state['last_error'] or 'немає'}"
    )

@router.message(Command("refresh"))
async def refresh_handler(message: Message):
    await message.answer("Оновлюю каталог. Це може зайняти кілька хвилин.")
    count = await refresh_catalog()
    if count:
        await message.answer(f"Готово. Збережено товарів: {count}.")
    else:
        await message.answer(
            "Каталог не завантажився. Надішліть команду /status і перевірте "
            "поле «Останній HTTP» та «Остання помилка»."
        )

@router.message(Command("searchtest"))
async def searchtest_handler(message: Message):
    query = message.text.partition(" ")[2].strip() or "шоколад"
    products = await search_products(query, 5)
    if not products:
        await message.answer(
            f"За запитом «{query}» нічого не знайдено. "
            "Спочатку виконайте /refresh, потім перевірте /status."
        )
        return
    await message.answer(
        "Знайдено:\n" + "\n".join(
            f"• {p['title']} — {p['price'] or 'ціна в картці'}"
            for p in products
        )
    )

@router.message(Command("delivery"))
@router.message(F.text == "🚚 Доставка й оплата")
async def delivery_handler(message: Message):
    await message.answer(
        "🚚 Актуальні умови доставки й оплати:\n"
        "https://la-torta.ua/ua/shipping-and-payment/",
        disable_web_page_preview=True,
    )

@router.message(Command("shops"))
@router.message(F.text == "📍 Магазини")
async def shops_handler(message: Message):
    await message.answer(
        "📍 Актуальні адреси та графік:\nhttps://la-torta.ua/ua/contacts/",
        disable_web_page_preview=True,
    )

@router.message(Command("catalog"))
@router.message(F.text == "🔎 Знайти товар")
async def catalog_handler(message: Message):
    await message.answer(
        "Напишіть назву або опишіть товар, наприклад:\n"
        "«червоний жиророзчинний барвник»."
    )

@router.message(Command("manager"))
@router.message(F.text == "👩‍💼 Покликати менеджера")
async def manager_handler(message: Message, bot: Bot):
    sent = False
    if MANAGER_CHAT_ID:
        user = message.from_user
        who = f"@{user.username}" if user and user.username else f"ID {message.chat.id}"
        try:
            await bot.send_message(
                MANAGER_CHAT_ID,
                f"🔔 Запит клієнта\nКлієнт: {who}\nПовідомлення: {message.text or '—'}",
            )
            sent = True
        except Exception:
            log.exception("Manager notification failed")
    await message.answer(
        "Я передала запит Вікторії." if sent else "Напишіть Вікторії за кнопкою нижче.",
        reply_markup=manager_keyboard(),
    )

@router.message(F.text)
async def chat_handler(message: Message, bot: Bot):
    text = message.text.strip()
    await save_message(message.chat.id, "user", text)
    await bot.send_chat_action(message.chat.id, "typing")

    products = await search_products(text, 4)

    try:
        answer = await ai_answer(message.chat.id, text, products)
    except Exception:
        log.exception("AI answer failed")
        answer = "Не вдалося сформувати AI-відповідь. Покажу знайдені товари."

    await save_message(message.chat.id, "assistant", answer)
    await message.answer(answer)

    for product in products[:3]:
        caption = f"<b>{product['title']}</b>"
        if product.get("price"):
            caption += f"\nЦіна: {product['price']}"
        caption += "\nАктуальність перевірте у картці товару."

        try:
            if product.get("image"):
                await message.answer_photo(
                    product["image"],
                    caption=caption,
                    reply_markup=product_keyboard(product["url"]),
                )
            else:
                await message.answer(
                    caption,
                    reply_markup=product_keyboard(product["url"]),
                )
        except Exception:
            await message.answer(
                caption,
                reply_markup=product_keyboard(product["url"]),
            )

async def catalog_scheduler():
    if not START_CATALOG_REFRESH:
        return
    await asyncio.sleep(15)
    while True:
        try:
            await refresh_catalog()
            await asyncio.sleep(REFRESH_HOURS * 3600)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("Catalog scheduler crashed")
            await asyncio.sleep(1800)

async def health_handler(request):
    return web.json_response({
        "ok": True,
        "products": await product_count(),
        "categories_found": catalog_state["categories_found"],
        "catalog_running": catalog_state["running"],
        "last_http_status": catalog_state["last_http_status"],
        "last_error": catalog_state["last_error"],
    })

async def start_http_server():
    app = web.Application()
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info("HTTP health server on port %s", PORT)
    return runner

async def main():
    if not TOKEN:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is missing")

    await init_db()
    runner = await start_http_server()

    bot = Bot(TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=False)
    await bot.set_my_commands([
        BotCommand(command="start", description="Почати"),
        BotCommand(command="refresh", description="Оновити каталог"),
        BotCommand(command="status", description="Статус каталогу"),
        BotCommand(command="searchtest", description="Тест пошуку"),
        BotCommand(command="catalog", description="Знайти товар"),
        BotCommand(command="manager", description="Покликати менеджера"),
        BotCommand(command="myid", description="Мій Telegram ID"),
    ])

    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    task = asyncio.create_task(catalog_scheduler())

    try:
        await dispatcher.start_polling(
            bot,
            allowed_updates=dispatcher.resolve_used_update_types(),
        )
    finally:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        await bot.session.close()
        await runner.cleanup()

if __name__ == "__main__":
    asyncio.run(main())
