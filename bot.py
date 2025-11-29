# bot.py — async + aiogram 3.7+, qty-алиасы, умный поиск колонок,
# расширенные статусы, двухшаговая схема область→район,
# разделение на "автотранспорт / прочее", вывод типов по области и по району
# (с разрезом по статусам в районе) + GPS-учёт по областям
# и раздельные состояния для автотранспорта и оборудования по области
# + раздельные состояния по категориям в «Общее по республике».

import asyncio
import logging
from datetime import datetime, timedelta
from pathlib import Path
import os
from typing import Dict, Tuple, Optional, List
import re
import time
import html
import random
import unicodedata
from collections import defaultdict

from aiogram import Bot, Dispatcher, types, F, BaseMiddleware
from aiogram.filters import Command
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.exceptions import TelegramBadRequest
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from dotenv import load_dotenv

# ---------- Логирование ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("bot.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ---------- .env / переменные окружения ----------
from dotenv import load_dotenv
import json

BASE_DIR = Path(__file__).resolve().parent

# Локально (на твоём компе) это прочитает .env.
# На Render — просто проигнорирует, и возьмёт значения из Environment.
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("Переменная BOT_TOKEN не найдена!")

# ---------- Google Sheets через GOOGLE_CREDS_JSON ----------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

google_creds_json = os.getenv("GOOGLE_CREDS_JSON")
if not google_creds_json:
    raise RuntimeError("Переменная GOOGLE_CREDS_JSON не найдена!")

try:
    creds_info = json.loads(google_creds_json)
    creds = Credentials.from_service_account_info(creds_info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    logger.info("Успешная авторизация в Google Sheets")
except Exception as e:
    logger.error(f"Ошибка авторизации Google Sheets: {e}")
    raise

# ---------- Конфигурация ----------
REGION_SHEETS = {
    "Андижон":             os.getenv("SHEET_ANDIJON"),
    "Фарғона":             os.getenv("SHEET_FARGONA"),
    "Наманган":            os.getenv("SHEET_NAMANGAN"),
    "Тошкент шаҳри":       os.getenv("SHEET_TASHKENT"),
    "Тошкент вил.":        os.getenv("SHEET_TASHKENT_VIL"),
    "Самарқанд":           os.getenv("SHEET_SAMARKAND"),
    "Жиззах":              os.getenv("SHEET_JIZZAKH"),
    "Сирдарё":             os.getenv("SHEET_SIRDARYO"),
    "Қашқадарё":           os.getenv("SHEET_QASHQADARYO"),
    "Сурхондарё":          os.getenv("SHEET_SURXONDARYO"),
    "Бухоро":              os.getenv("SHEET_BUKHARA"),
    "Навоий":              os.getenv("SHEET_NAVOIY"),
    "Хоразм":              os.getenv("SHEET_XORAZM"),
    "Қорақалпоғистон":     os.getenv("SHEET_QORAQALPOG"),
    "Дамхужа":             os.getenv("SHEET_DAMXOJA"),
    "Мусаффо":             os.getenv("SHEET_MUSAFFO"),
    "Чимган-Чарбоғ":       os.getenv("SHEET_CHIMGAN"),
    "Сувўлчагичхизмати":   os.getenv("SHEET_SUVULCHAGICH"),
}

# Колонки «по умолчанию»
COL_TYPE   = "Техника тури"
COL_REGION = "Бириктирилган шахар ёки туман"
COL_QTY    = "№ Т/р"   # НЕ используем как qty, оставлено для совместимости
COL_STATUS = "Холати"

# Алиасы
STATUS_ALIASES = [
    "Холати", "Ҳолати", "Статус", "Состояние", "Ҳолат", "Holati", "Status",
    "Техника холати", "Техника ҳолати", "Техника статусы", "Техника состояние"
]
TYPE_ALIASES   = [COL_TYPE, "Техники тури", "Тип техники", "Тури техника", "Техника тўри", "Вид техники", "Наименование техники"]
REGION_ALIASES = [COL_REGION, "Город/район", "Район", "Туман", "Населенный пункт", "Шахар/туман", "Шахар ёки туман"]
QTY_ALIASES    = ["Количество", "Кол-во", "Сони", "Qty", "Count"]

# Алиасы для колонки с GPS/трекером
TRACKER_ALIASES = [
    "GPS",
    "Gps",
    "gps",
    "Трекер",
    "Трекер установлен",
    "Наличие трекера",
    "Наличие GPS",
    "GPS трекер",
    "GPS-трекер",
    "GPS / ГЛОНАСС",
    "GPS/ГЛОНАСС",
    "GPS билан таъминланганлиги",
    "GPS билапн таъминланганлиги",
]

# ---------- Кеш/квоты ----------
CACHE: Dict[str, Tuple[pd.DataFrame, datetime]] = {}
CACHE_TTL = timedelta(minutes=30)
LAST_API_CALL = datetime.min
API_DELAY = 2.0
PARALLEL_LIMIT = 4

# ---------- Callback-сокращения ----------
CB_MAP: Dict[str, str] = {}  # id -> payload


def put_cb(payload: str) -> str:
    key = str(abs(hash(payload)))
    CB_MAP[key] = payload
    return key


def get_cb(key: str) -> Optional[str]:
    return CB_MAP.get(key)


# ---------- Хелперы ----------
def esc(s: str) -> str:
    return html.escape(str(s or ""))


def _normalize_header(s: str) -> str:
    s = str(s or "").strip().lower()
    s = s.replace("ҳ", "х")  # Ҳ≈Х
    s = re.sub(r"\s+", "", s)
    return s


def _norm_map(cols: List[str]) -> Dict[str, str]:
    return {_normalize_header(c): c for c in cols}


def find_column(df: pd.DataFrame, aliases: List[str]) -> Optional[str]:
    """
    1) точное совпадение (после нормализации),
    2) подстрока (берём самый короткий матч),
    3) эвристика: токены для статуса/региона/типа.
    """
    norm_cols = _norm_map(list(df.columns))

    # 1) точное
    for a in aliases:
        k = _normalize_header(a)
        if k in norm_cols:
            return norm_cols[k]

    # 2) подстрока
    best = None
    for a in aliases:
        ka = _normalize_header(a)
        for nc, orig in norm_cols.items():
            if ka and ka in nc:
                if best is None or len(nc) < len(_normalize_header(best)):
                    best = orig
    if best:
        return best

    # 3) эвристика
    token_sets = [
        {"холат", "ҳолат", "holat", "status", "состояние"},   # статус
        {"шахар", "туман", "район", "город"},                 # регион/нас.пункт
        {"техника", "тип", "тур", "вид"},                     # тип техники
    ]
    for tokens in token_sets:
        for nc, orig in norm_cols.items():
            if any(tok in nc for tok in tokens):
                return orig

    return None


APOSTS = {"'", "ʼ", "’", "ʹ", "′", "`", "´", "ʽ", "ꞌ", "ʻ"}


def _prenorm(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    for a in APOSTS:
        s = s.replace(a, "'")
    return s.replace("\u00A0", " ")


# ---- Нормализация типа техники ----
def normalize_tech_type(tech_type: str) -> Optional[str]:
    if not isinstance(tech_type, str) or tech_type.strip() == "":
        return None
    raw = tech_type
    normalized = _prenorm(tech_type).strip().lower()
    normalized = re.sub(r"[^\w\s]", " ", normalized)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    if not normalized or normalized in ["nan", "none", "null", ""]:
        return None

    # отбрасываем строковые итоги / служебные строки
    summary_keywords = ["жами", "итого", "барчаси", "всего", "jami", "all"]
    if any(k in normalized for k in summary_keywords):
        return None

    type_mapping = {
        "погрузчик": "Погрузчик",
        "мини бортовой": "Мини бортовой",
        "минии бортовой": "Мини бортовой",
        "микро бортовой": "Мини бортовой",
        "эвакуатор": "Эвакуатор",
        "хлоровоз": "Хлоровоз",
        "самосвал": "Самосвал",
        "экскаватор": "Экскаватор",
        "трактор": "Трактор",
        "бульдозер": "Бульдозер",
        "автокран": "Автокран",
        "бетономешалка": "Бетономешалка",
        "цистерна": "Цистерна",
        "фургон": "Фургон",
        "рефрижератор": "Рефрижератор",
        "гидролинамическая": "Гидродинамическая",
        "гидролинамический": "Гидродинамическая",
        "гидродинамическая": "Гидродинамическая",
        "гидродинамический": "Гидродинамическая",
        "гидравлическая": "Гидродинамическая",
        "лаболаторная": "Лабораторная",
        "лобораторная": "Лабораторная",
        "лабораторная": "Лабораторная",
        "камаз": "Камаз",
        "зил": "ЗИЛ",
        "газель": "ГАЗель",
        "уаз": "УАЗ",
        "компрессор": "Компрессор",
        "генератор": "Генератор",
        "автобус": "Автобус",
        "микроавтобус": "Микроавтобус",
        "машина": "Машина",
        "грузовик": "Грузовик",
    }
    for key, standard_name in type_mapping.items():
        if key in normalized:
            return standard_name
    return raw.strip().capitalize()


# ---- Нормализация статуса ----
def normalize_status(status: str) -> str:
    if not isinstance(status, str) or status.strip() == "":
        return "Холати номаълум"

    s = _prenorm(status).strip().lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()

    # Иконки/ярлыки
    if any(tok in s for tok in ["✅", "🟢", "green", "ok"]):
        return "Ярокли"
    if any(tok in s for tok in ["⛔", "🛑", "🔴", "❌"]):
        return "Яроксиз"
    if any(tok in s for tok in ["🛠", "🟡", "⚠"]):
        return "Таъмирталаб"

    # Работает
    if (
        "яроқли" in s or "ярокли" in s
        or "ишлайди" in s or "ишламоқда" in s or "ишлаб турибди" in s
        or "ишга яроқли" in s or "ишга ярокли" in s
        or "эксплуатацияда" in s or "фойдаланишда" in s or "в эксплуатации" in s
        or "operational" in s or "in service" in s or "working" in s
        or re.search(r"\bв\s*рабоч(ем|ее)?\s*состояни(и|е)\b", s)
        or "исправен" in s or "исправна" in s or "исправно" in s or "исправный" in s
        or "работает" in s or "ready" in s or "good" in s or "active" in s
    ):
        return "Ярокли"

    # Не работает
    if (
        "яроқсиз" in s or "яроксиз" in s
        or "ишламаяпти" in s or "ишламайди" in s or "ишламаган" in s
        or "ишдан чиққан" in s or "ишдан чиккан" in s or "тизимдан ташқари" in s
        or "не работает" in s or "в не исправн" in s or "вне исправн" in s
        or "broken" in s or "inactive" in s or "out of order" in s
        or "қисман ишламайди" in s or "частично не работает" in s
        or ("исправен" in s and "не" in s)
        or "неисправ" in s
    ):
        return "Яроксиз"

    # Ремонт/обслуживание
    if (
        "таъмирталаб" in s or "тамирталаб" in s
        or "таъмирда" in s or "ремонтда" in s
        or "ремонт" in s or "ремонтируется" in s
        or "на ремонте" in s or re.search(r"\bв\s*ремонтн", s)
        or "обслуживан" in s or "техобслуж" in s
        or "maintenance" in s or "under repair" in s or "repair" in s
    ):
        return "Таъмирталаб"

    return "Холати номаълум"


def get_status_emoji(status: str) -> str:
    mapping = {
        "Ярокли": "🟩",
        "Таъмирталаб": "🟨",
        "Яроксиз": "🟥",
    }
    return mapping.get(status, "")


# ---- Нормализация признака GPS (трекера) ----
def normalize_tracker_flag(value) -> bool:
    if value is None:
        return False

    s = str(value).strip()
    if s == "":
        return False

    try:
        num = float(s.replace(",", "."))
        if num > 0:
            return True
        if num == 0:
            return False
    except ValueError:
        pass

    s_low = s.lower()

    if "мавжуд эмас" in s_low or "mavjud emas" in s_low:
        return False

    if "мавжуд" in s_low or "mavjud" in s_low:
        return True

    if s_low in ["no", "нет", "yo'q", "йўқ", "yuk", "йук", "0", "false", "n"]:
        return False

    if s_low in ["yes", "да", "ha", "bor", "есть", "1", "true", "y", "д"]:
        return True

    if "gps" in s_low or "трекер" in s_low:
        if "нет" not in s_low and "yo'q" not in s_low and "йўқ" not in s_low:
            return True

    return False


# ---------- Безопасная задержка (async) ----------
async def async_safe_api_call():
    global LAST_API_CALL
    now = datetime.now()
    elapsed = (now - LAST_API_CALL).total_seconds()
    if elapsed < API_DELAY:
        await asyncio.sleep((API_DELAY - elapsed) + random.uniform(0, 0.4))
    LAST_API_CALL = datetime.now()


# ---------- Синхронная загрузка региона (для to_thread) ----------
def load_single_region_sync(region_sheet: Tuple[str, str]) -> Tuple[str, pd.DataFrame]:
    region_name, sheet_id = region_sheet
    if not sheet_id:
        logger.warning(f"Нет sheet_id для региона {region_name}")
        return region_name, pd.DataFrame()

    try:
        sh = gc.open_by_key(sheet_id)
        ws = sh.sheet1
        values = ws.get_all_records()
        df = pd.DataFrame(values)

        if df.empty:
            logger.info(f"Регион {region_name}: таблица пустая")
            return region_name, pd.DataFrame()

        df.columns = [str(c).strip() for c in df.columns]

        status_col = find_column(df, STATUS_ALIASES)
        type_col   = find_column(df, TYPE_ALIASES)
        region_col = find_column(df, REGION_ALIASES)

        if status_col:
            raw_vc = df[status_col].astype(str).str.strip().replace("", "∅").value_counts().head(15)
            logger.info(f"🔎 {region_name}: TOP статусов (raw): {raw_vc.to_dict()}")

        qty_col = find_column(df, QTY_ALIASES)
        if qty_col:
            df["qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int)
            df.loc[df["qty"] <= 0, "qty"] = 1
        else:
            df["qty"] = 1

        df["region_name"] = region_name

        if type_col:
            df["type_normalized"] = df[type_col].apply(normalize_tech_type)
            before = len(df)
            df = df[df["type_normalized"].notna() & (df["type_normalized"] != "")]
            after = len(df)
            if before != after:
                logger.info(f"📝 {region_name}: отфильтровано {before - after} строк с пустыми/служебными типами")
        else:
            logger.warning(f"⚠️ {region_name}: колонка типа техники не найдена")
            return region_name, pd.DataFrame()

        if status_col:
            df[status_col] = df[status_col].fillna("")
            df["status_normalized"] = df[status_col].apply(normalize_status)
            logger.info(f"🔍 {region_name}: нормализованные статусы: {df['status_normalized'].value_counts().to_dict()}")
        else:
            df["status_normalized"] = "Холати номаълум"
            logger.warning(f"⚠️ {region_name}: колонка статуса не найдена")

        if region_col:
            df["city_district"] = df[region_col].fillna("Не указан")
            df["city_district"] = df["city_district"].apply(
                lambda x: region_name if x == "Не указан" or str(x).strip() == "" else str(x).strip()
            )
        else:
            df["city_district"] = region_name
            logger.warning(f"ℹ️ {region_name}: колонка города/района не найдена")

        tracker_col = find_column(df, TRACKER_ALIASES)
        if tracker_col:
            df["has_tracker"] = df[tracker_col].apply(normalize_tracker_flag)
        else:
            df["has_tracker"] = False

        keep = ["region_name", "city_district", "type_normalized", "status_normalized", "qty", "has_tracker"]
        df = df[keep]

        total_qty = int(df["qty"].sum())
        status_stats = df.groupby("status_normalized")["qty"].sum()
        logger.info(
            "✓ %s: %s строк, %s ед. [%s]",
            region_name,
            len(df),
            total_qty,
            ", ".join([f"{k}: {int(v)}" for k, v in status_stats.items()])
        )
        return region_name, df

    except Exception as e:
        if "429" in str(e):
            logger.warning(f"⏳ Лимит API для {region_name}, жду 10 секунд (в потоке)...")
            time.sleep(10)
            try:
                sh = gc.open_by_key(sheet_id)
                ws = sh.sheet1
                values = ws.get_all_records()
                df = pd.DataFrame(values)
                if df.empty:
                    return region_name, pd.DataFrame()

                df.columns = [str(c).strip() for c in df.columns]
                status_col = find_column(df, STATUS_ALIASES)
                type_col   = find_column(df, TYPE_ALIASES)
                region_col = find_column(df, REGION_ALIASES)

                qty_col = find_column(df, QTY_ALIASES)
                if qty_col:
                    df["qty"] = pd.to_numeric(df[qty_col], errors="coerce").fillna(0).astype(int)
                    df.loc[df["qty"] <= 0, "qty"] = 1
                else:
                    df["qty"] = 1

                df["region_name"] = region_name

                if type_col:
                    df["type_normalized"] = df[type_col].apply(normalize_tech_type)
                    df = df[df["type_normalized"].notna() & (df["type_normalized"] != "")]
                else:
                    return region_name, pd.DataFrame()

                if status_col:
                    df[status_col] = df[status_col].fillna("")
                    df["status_normalized"] = df[status_col].apply(normalize_status)
                else:
                    df["status_normalized"] = "Холати номаълум"

                if region_col:
                    df["city_district"] = df[region_col].fillna("Не указан")
                    df["city_district"] = df["city_district"].apply(
                        lambda x: region_name if x == "Не указан" or str(x).strip() == "" else str(x).strip()
                    )
                else:
                    df["city_district"] = region_name

                tracker_col = find_column(df, TRACKER_ALIASES)
                if tracker_col:
                    df["has_tracker"] = df[tracker_col].apply(normalize_tracker_flag)
                else:
                    df["has_tracker"] = False

                df = df[["region_name", "city_district", "type_normalized", "status_normalized", "qty", "has_tracker"]]
                logger.info(f"✓ {region_name}: повторная загрузка успешна")
                return region_name, df

            except Exception as retry_error:
                logger.error(f"✗ {region_name}: ошибка при повторной загрузке - {retry_error}")
                return region_name, pd.DataFrame()
        else:
            logger.error(f"✗ {region_name}: ошибка загрузки - {e}")
            return region_name, pd.DataFrame()


# ---------- Асинхронные обёртки ----------
sem = asyncio.Semaphore(PARALLEL_LIMIT)


async def load_single_region_async(region_sheet: Tuple[str, str]) -> Tuple[str, pd.DataFrame]:
    async with sem:
        await async_safe_api_call()
        return await asyncio.to_thread(load_single_region_sync, region_sheet)


async def load_all_regions_async() -> pd.DataFrame:
    regions = [(name, sid) for name, sid in REGION_SHEETS.items() if sid]
    if not regions:
        return pd.DataFrame(columns=["region_name", "city_district", "type_normalized", "status_normalized", "qty", "has_tracker"])

    tasks = [load_single_region_async(rs) for rs in regions]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    parts: List[pd.DataFrame] = []
    ok = 0
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Ошибка при загрузке региона: {r}")
            continue
        name, df = r
        if not df.empty:
            parts.append(df)
            ok += 1
        else:
            logger.warning(f"❌ {name}: нет данных после фильтрации")

    if parts:
        df = pd.concat(parts, ignore_index=True)
        total_qty = int(df["qty"].sum())
        logger.info(f"📊 ИТОГО: {len(df)} строк, {total_qty} ед. из {ok} регионов")
        return df

    logger.error("❌ Не удалось загрузить данные ни из одного региона")
    return pd.DataFrame(columns=["region_name", "city_district", "type_normalized", "status_normalized", "qty", "has_tracker"])


# ---- Кеш (async) ----
async def get_df_async(region: Optional[str] = None, force_refresh: bool = False) -> pd.DataFrame:
    key = region or "ALL"
    now = datetime.now()

    if not force_refresh and key in CACHE and now < CACHE[key][1]:
        return CACHE[key][0].copy()

    try:
        if region and region != "ALL":
            _, df = await load_single_region_async((region, REGION_SHEETS.get(region)))
        else:
            df = await load_all_regions_async()

        CACHE[key] = (df.copy(), now + CACHE_TTL)
        return df
    except Exception as e:
        logger.error(f"❌ Критическая ошибка в get_df_async: {e}")
        if key in CACHE:
            return CACHE[key][0].copy()
        return pd.DataFrame(columns=["region_name", "city_district", "type_normalized", "status_normalized", "qty", "has_tracker"])


# ---- Агрегаты и форматирование ----
def count_type_per_region(df_all: pd.DataFrame, tech_type: str) -> pd.DataFrame:
    if df_all.empty:
        return pd.DataFrame(columns=["region_name", "qty"])
    try:
        normalized_type = normalize_tech_type(tech_type)
        if not normalized_type:
            return pd.DataFrame(columns=["region_name", "qty"])
        sub = df_all.loc[df_all["type_normalized"] == normalized_type].copy()
        if sub.empty:
            return pd.DataFrame(columns=["region_name", "qty"])
        return (
            sub
            .groupby("region_name", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в count_type_per_region: {e}")
        return pd.DataFrame(columns=["region_name", "qty"])


def get_status_distribution_for_type_region(df_all: pd.DataFrame, tech_type: str, region: str) -> pd.DataFrame:
    if df_all.empty:
        return pd.DataFrame(columns=["status_normalized", "qty"])
    try:
        normalized_type = normalize_tech_type(tech_type)
        if not normalized_type:
            return pd.DataFrame(columns=["status_normalized", "qty"])
        mask = (df_all["region_name"] == region) & (df_all["type_normalized"] == normalized_type)
        filtered = df_all.loc[mask].copy()
        if filtered.empty:
            return pd.DataFrame(columns=["status_normalized", "qty"])
        return (
            filtered
            .groupby("status_normalized", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в get_status_distribution_for_type_region: {e}")
        return pd.DataFrame(columns=["status_normalized", "qty"])


def get_detailed_city_status(df_all: pd.DataFrame, tech_type: str, region: str) -> Dict[str, Dict[str, int]]:
    if df_all.empty:
        return {}
    try:
        normalized_type = normalize_tech_type(tech_type)
        if not normalized_type:
            return {}
        mask = (df_all["region_name"] == region) & (df_all["type_normalized"] == normalized_type)
        filtered = df_all.loc[mask].copy()
        if filtered.empty:
            return {}
        city_status: Dict[str, Dict[str, int]] = {}
        for city in filtered["city_district"].unique():
            city_data = filtered[filtered["city_district"] == city]
            status_counts: Dict[str, int] = {}
            for st in ["Ярокли", "Яроксиз", "Таъмирталаб", "Холати номаълум"]:
                cnt = int(city_data.loc[city_data["status_normalized"] == st, "qty"].sum())
                if cnt > 0:
                    status_counts[st] = cnt
            total_city = sum(status_counts.values())
            if total_city > 0:
                city_status[str(city) if str(city).strip() else "Не указан"] = status_counts
        return city_status
    except Exception as e:
        logger.error(f"❌ Ошибка в get_detailed_city_status: {e}")
        return {}


def all_types_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=[COL_TYPE, "qty"])
    try:
        valid = df[df["type_normalized"].notna()].copy()
        if valid.empty:
            return pd.DataFrame(columns=[COL_TYPE, "qty"])
        return (
            valid
            .groupby("type_normalized", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
            .rename(columns={"type_normalized": COL_TYPE})
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в all_types_summary: {e}")
        return pd.DataFrame(columns=[COL_TYPE, "qty"])


def get_region_counts(df_all: pd.DataFrame) -> Dict[str, int]:
    if df_all.empty:
        return {region: 0 for region in REGION_SHEETS.keys()}
    try:
        valid = df_all[df_all["type_normalized"].notna()].copy()
        stats = valid.groupby("region_name", as_index=False).agg(qty=("qty", "sum"))
        as_dict = dict(zip(stats["region_name"], stats["qty"]))
        return {region: int(as_dict.get(region, 0)) for region in REGION_SHEETS.keys()}
    except Exception as e:
        logger.error(f"❌ Ошибка в get_region_counts: {e}")
        return {region: 0 for region in REGION_SHEETS.keys()}


def create_regions_keyboard(df_all: pd.DataFrame) -> types.InlineKeyboardMarkup:
    try:
        region_counts = get_region_counts(df_all)
        total = int(df_all[df_all["type_normalized"].notna()]["qty"].sum()) if not df_all.empty else 0
        kb = InlineKeyboardBuilder()
        for region in REGION_SHEETS.keys():
            kb.button(text=f"{region} ({region_counts.get(region, 0)} ед.)", callback_data=f"region:{region}")
        kb.button(text=f"Все области ({total} ед.)", callback_data="region:ALL")
        kb.adjust(2)
        return kb.as_markup()
    except Exception as e:
        logger.error(f"❌ Ошибка создания клавиатуры областей: {e}")
        kb = InlineKeyboardBuilder()
        for region in REGION_SHEETS.keys():
            kb.button(text=region, callback_data=f"region:{region}")
        kb.button(text="Все области", callback_data="region:ALL")
        kb.adjust(2)
        return kb.as_markup()


def create_types_keyboard(types_df: pd.DataFrame) -> Tuple[types.InlineKeyboardMarkup, int]:
    kb = InlineKeyboardBuilder()
    valid_types = 0
    for _, row in types_df.iterrows():
        tech_type = row[COL_TYPE]
        count = int(row["qty"])
        if tech_type and str(tech_type).strip() and str(tech_type).strip().lower() != "nan":
            payload = f"count_type:{tech_type}"
            cbid = put_cb(payload)
            kb.button(text=f"{tech_type} ({count} ед.)", callback_data=cbid)
            valid_types += 1
    kb.button(text="↩️ Главное меню", callback_data="main_menu")
    kb.adjust(1)
    return kb.as_markup(), valid_types


def summarize_overall_status(status_df: pd.DataFrame) -> str:
    counts = {"Ярокли": 0, "Таъмирталаб": 0, "Яроксиз": 0, "Холати номаълум": 0}
    if not status_df.empty:
        for _, row in status_df.iterrows():
            st = str(row["status_normalized"])
            qty = int(row["qty"])
            if st in counts:
                counts[st] = qty
    return f"🟩 {counts['Ярокли']} | 🟨 {counts['Таъмирталаб']} | 🟥 {counts['Яроксиз']}"


def fmt_status_distribution(status_df: pd.DataFrame) -> str:
    if status_df.empty:
        return "⚙️ Нет данных о состояниях техники"
    lines: List[str] = []
    total = 0
    for _, row in status_df.iterrows():
        status = row["status_normalized"]
        qty = int(row["qty"])
        total += qty
        emoji = get_status_emoji(status)
        if emoji:
            lines.append(f"{emoji} <b>{esc(status)}</b> — {qty} ед.")
        else:
            lines.append(f"<b>{esc(status)}</b> — {qty} ед.")
    lines.append(f"\n<b>📊 Всего:</b> {total} ед.")
    return "\n".join(lines)


def fmt_detailed_city_status(city_status_data: Dict[str, Dict[str, int]]) -> str:
    if not city_status_data:
        return "🏙️ Нет данных по городам/районам"

    rows = []
    for city, cnt in city_status_data.items():
        g = int(cnt.get("Ярокли", 0))
        y = int(cnt.get("Таъмирталаб", 0))
        r = int(cnt.get("Яроксиз", 0))
        k = int(cnt.get("Холати номаълум", 0))
        total = g + y + r + k
        rows.append((city, total, g, y, r, k))

    rows.sort(key=lambda t: (-t[4], -t[3], t[0].lower()))
    lines: List[str] = []
    for city, total, g, y, r, k in rows:
        parts: List[str] = []
        if g > 0:
            parts.append(f"🟩 {g}")
        if y > 0:
            parts.append(f"🟨 {y}")
        if r > 0:
            parts.append(f"🟥 {r}")
        status_line = " | ".join(parts) if parts else "Нет данных"
        lines.append(
            f"📍 <b>{esc(city)}</b> — {total} ед.\n"
            f"{status_line}"
        )
    return "\n\n".join(lines)


def fmt_table(df: pd.DataFrame, left_col: str, max_lines: int = 100) -> str:
    if df.empty:
        return "📭 Данных нет"

    lines: List[str] = []
    for _, row in df.iterrows():
        value = row.get(left_col)
        if pd.isna(value) or str(value).strip() == "":
            value = "Не указано"
        qty = int(row.get("qty", 0))
        lines.append(f"• {esc(value)} — {qty} ед.")

    if len(lines) > max_lines:
        tail = len(lines) - max_lines
        lines = lines[:max_lines] + [f"… и ещё {tail} строк"]

    return "\n".join(lines)


def fmt_types_with_statuses(df_subset: pd.DataFrame, max_types: int = 50) -> str:
    if df_subset.empty or "type_normalized" not in df_subset.columns:
        return "📭 Данных о типах техники нет"

    try:
        agg = (
            df_subset
            .groupby(["type_normalized", "status_normalized"], as_index=False)
            .agg(qty=("qty", "sum"))
        )

        totals = (
            agg
            .groupby("type_normalized", as_index=False)
            .agg(total=("qty", "sum"))
            .sort_values("total", ascending=False)
        )

        lines: List[str] = []
        shown = 0

        for _, row in totals.iterrows():
            t = row["type_normalized"]
            if pd.isna(t) or str(t).strip() == "":
                t = "Не указано"

            t_rows = agg[agg["type_normalized"] == row["type_normalized"]]

            g = y = r = k = 0
            for _, tr in t_rows.iterrows():
                st = tr["status_normalized"]
                q = int(tr["qty"])
                if st == "Ярокли":
                    g += q
                elif st == "Таъмирталаб":
                    y += q
                elif st == "Яроксиз":
                    r += q
                else:
                    k += q

            parts: List[str] = []
            if g > 0:
                parts.append(f"🟩 {g}")
            if y > 0:
                parts.append(f"🟨 {y}")
            if r > 0:
                parts.append(f"🟥 {r}")
            if k > 0:
                parts.append(f"⬛ {k}")

            status_line = " | ".join(parts) if parts else "Нет данных"

            lines.append(
                f"• <b>{esc(t)}</b> — {int(row['total'])} ед.\n"
                f"   {status_line}"
            )

            shown += 1
            if shown >= max_types:
                if len(totals) > max_types:
                    lines.append(f"… и ещё {len(totals) - max_types} типов")
                break

        return "\n".join(lines) if lines else "📭 Данных о типах техники нет"
    except Exception as e:
        logger.error(f"❌ Ошибка в fmt_types_with_statuses: {e}")
        return "⚠️ Ошибка при формировании списка типов"


# ---- Категории: автотранспорт / прочее оборудование ----
def is_equipment_type(type_name: str) -> bool:
    """
    True, если тип относится к прицепам/цистернам/насосам/компрессорам/генераторам и т.п.
    По твоему описанию для Андижана:
      'прицеп', 'САК', 'цистерна', 'компрессор', 'генератор' — оборудование.
    Расширим это правило и для других областей.
    """
    if not isinstance(type_name, str):
        return False
    s = type_name.lower()

    equipment_keywords = [
        "прицеп",
        "сак",           # САК — считаем оборудованием
        "цистерн",
        "насос",
        "компрессор",
        "генератор",
        "агрегат",
        "мотопомп",
        "насосная",
        "насос станц",
    ]
    return any(k in s for k in equipment_keywords)


def summarize_region_categories(df_region: pd.DataFrame) -> Tuple[int, int, int]:
    if df_region.empty:
        return 0, 0, 0

    types = df_region["type_normalized"].astype(str)
    mask_equipment = types.apply(is_equipment_type)

    auto_qty = int(df_region.loc[~mask_equipment, "qty"].sum())
    equip_qty = int(df_region.loc[mask_equipment, "qty"].sum())
    total = auto_qty + equip_qty
    return auto_qty, equip_qty, total


def summarize_district_categories(df_region: pd.DataFrame, district: str) -> Tuple[int, int, int]:
    sub = df_region[df_region["city_district"] == district]
    if sub.empty:
        return 0, 0, 0

    types = sub["type_normalized"].astype(str)
    mask_equipment = types.apply(is_equipment_type)

    auto_qty = int(sub.loc[~mask_equipment, "qty"].sum())
    equip_qty = int(sub.loc[mask_equipment, "qty"].sum())
    total = auto_qty + equip_qty
    return auto_qty, equip_qty, total


def summarize_republic_categories(df_all: pd.DataFrame) -> Tuple[int, int, int]:
    if df_all.empty or "type_normalized" not in df_all.columns:
        return 0, 0, 0

    types = df_all["type_normalized"].astype(str)
    mask_equipment = types.apply(is_equipment_type)

    auto_qty = int(df_all.loc[~mask_equipment, "qty"].sum())
    equip_qty = int(df_all.loc[mask_equipment, "qty"].sum())
    total = auto_qty + equip_qty
    return auto_qty, equip_qty, total


def get_status_distribution_any(df_subset: pd.DataFrame) -> pd.DataFrame:
    if df_subset.empty or "status_normalized" not in df_subset.columns:
        return pd.DataFrame(columns=["status_normalized", "qty"])
    try:
        return (
            df_subset
            .groupby("status_normalized", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в get_status_distribution_any: {e}")
        return pd.DataFrame(columns=["status_normalized", "qty"])


def create_districts_keyboard(df_region: pd.DataFrame, region: str) -> types.InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()

    if not df_region.empty:
        dist_df = (
            df_region
            .groupby("city_district", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
        )
        for _, row in dist_df.iterrows():
            city = str(row["city_district"]) if str(row["city_district"]).strip() else "Не указан"
            qty = int(row["qty"])
            payload = f"district:{region}|{city}"
            cbid = put_cb(payload)
            kb.button(text=f"{city} ({qty} ед.)", callback_data=cbid)

    kb.button(text="↩️ Назад к областям", callback_data="back_to_regions")
    kb.adjust(2, 1)
    return kb.as_markup()


# ---- GPS-учёт по областям ----
def count_trackers_by_region(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty or "has_tracker" not in df_all.columns:
        return pd.DataFrame(columns=["region_name", "qty"])
    try:
        sub = df_all[df_all["has_tracker"]]
        if sub.empty:
            return pd.DataFrame(columns=["region_name", "qty"])
        return (
            sub
            .groupby("region_name", as_index=False)
            .agg(qty=("qty", "sum"))
            .sort_values("qty", ascending=False)
        )
    except Exception as e:
        logger.error(f"❌ Ошибка в count_trackers_by_region: {e}")
        return pd.DataFrame(columns=["region_name", "qty"])


async def safe_edit_message(message: types.Message, text: str, **kwargs) -> None:
    try:
        await message.edit_text(text, **kwargs)
    except TelegramBadRequest as e:
        if "message is too long" in str(e):
            parts = [text[i:i+4000] for i in range(0, len(text), 4000)]
            await message.edit_text(parts[0], **kwargs)
            for part in parts[1:]:
                await message.answer(part)
        else:
            raise e


def main_menu_kb() -> types.ReplyKeyboardMarkup:
    kb = ReplyKeyboardBuilder()
    buttons = [
        "📍 Области",
        "📊 Общее по республике",
        "🔎 Посчитать по типу",
        "📡 GPS-учёт по областям",
        "🔄 Обновить кеш",
        "ℹ️ Помощь",
    ]
    for txt in buttons:
        kb.button(text=txt)
    kb.adjust(2)
    return kb.as_markup(resize_keyboard=True)


# ---- Rate Limit middleware (защита от спама) ----
class RateLimitMiddleware(BaseMiddleware):
    def __init__(self, limit: int = 5, per: float = 1.0):
        super().__init__()
        self.limit = limit
        self.per = per
        self._users = defaultdict(list)

    async def __call__(self, handler, event, data):
        from_user = getattr(event, "from_user", None)
        if not from_user:
            return await handler(event, data)

        user_id = from_user.id
        now = time.monotonic()

        timestamps = self._users[user_id]
        while timestamps and now - timestamps[0] > self.per:
            timestamps.pop(0)

        if len(timestamps) >= self.limit:
            # можно по желанию отправлять предупреждение
            return

        timestamps.append(now)
        return await handler(event, data)


# ---------- Обработчики ----------
async def main():
    bot = Bot(
        BOT_TOKEN,
        default=DefaultBotProperties(
            parse_mode=ParseMode.HTML,
            link_preview_is_disabled=False,
            protect_content=False
        )
    )
    dp = Dispatcher()

    # защита от спама на пользователя
    rate_mw = RateLimitMiddleware(limit=5, per=1.0)
    dp.message.middleware(rate_mw)
    dp.callback_query.middleware(rate_mw)

    @dp.message(Command("start"))
    async def start_cmd(m: types.Message):
        await m.answer(
            "🤖 <b>Бот для учета техники</b>\n\n"
            "✨ <i>Детальные состояния по областям и районам</i>\n\n"
            "Главное меню:\n"
            "• <b>📍 Области</b> — выбрать область и посмотреть сводку + районы\n"
            "• <b>📊 Общее по республике</b> — сводка по всем областям\n"
            "• <b>🔎 Посчитать по типу</b> — распределение по областям\n"
            "• <b>📡 GPS-учёт по областям</b> — сводка по наличию GPS у техники\n"
            "• <b>🔄 Обновить кеш</b> — обновить данные\n"
            "• <b>ℹ️ Помощь</b> — подробная справка",
            reply_markup=main_menu_kb()
        )

    @dp.message(F.text == "ℹ️ Помощь")
    async def help_cmd(m: types.Message):
        await m.answer(
            "📖 <b>Подробная справка</b>\n\n"
            "Этот бот показывает учёт техники по областям/районам с разбивкой по состояниям.\n\n"
            "🔘 <b>Кнопки главного меню</b>:\n"
            "• <b>📍 Области</b>\n"
            "  1) Выбираешь область.\n"
            "  2) Бот показывает:\n"
            "     • Автотранспорт / Прочая техника и оборудование / Всего\n"
            "     • Отдельно состояния автотранспорта и оборудования\n"
            "     • Список типов техники в области\n"
            "     • Список районов с количеством техники.\n"
            "  3) При выборе района бот отправляет отдельное сообщение:\n"
            "     • Кол-во автотранспорта\n"
            "     • Кол-во прочей техники\n"
            "     • Всего\n"
            "     • Общие состояния по району\n"
            "     • Типы техники по району с разрезом по статусам.\n\n"
            "• <b>📊 Общее по республике</b>\n"
            "  Показывает итоги по республике с делением на автотранспорт и прочую технику,\n"
            "  а также раздельное распределение по состояниям.\n\n"
            "• <b>🔎 Посчитать по типу</b>\n"
            "  Сначала выбираешь тип, затем бот показывает распределение по областям и детали.\n\n"
            "• <b>📡 GPS-учёт по областям</b>\n"
            "  Показывает, сколько техники с GPS есть в каждой области.\n\n"
            "• <b>🔄 Обновить кеш</b>\n"
            "  Очищает кеш данных. Следующий запрос подтянет свежие данные из Google Sheets.\n"
        )

    @dp.message(F.text == "📍 Области")
    async def choose_region(m: types.Message):
        try:
            loading_msg = await m.answer("🔄 Загружаю данные...")
            df_all = await get_df_async(None, force_refresh=False)
            kb = create_regions_keyboard(df_all)
            await loading_msg.delete()
            await m.answer("📍 Выбери область:", reply_markup=kb)
        except Exception as e:
            logger.error(f"❌ Ошибка в choose_region: {e}")
            await m.answer("❌ Ошибка при загрузке данных")

    @dp.callback_query(F.data.startswith("region:"))
    async def show_region_summary(c: types.CallbackQuery):
        try:
            await c.answer("📊 Загружаю данные по области...")
            region = c.data.split(":", 1)[1]

            # Все области
            if region == "ALL":
                df = await get_df_async(None, force_refresh=False)
                if df.empty:
                    await c.message.edit_text("❌ Нет данных для отображения")
                    return

                auto_qty, equip_qty, total = summarize_republic_categories(df)
                types_df = all_types_summary(df)

                if df.empty or "type_normalized" not in df.columns:
                    auto_df = pd.DataFrame(columns=df.columns)
                    equip_df = pd.DataFrame(columns=df.columns)
                else:
                    types = df["type_normalized"].astype(str)
                    mask_equipment = types.apply(is_equipment_type)
                    auto_df = df[~mask_equipment].copy()
                    equip_df = df[mask_equipment].copy()

                status_auto = get_status_distribution_any(auto_df)
                status_equip = get_status_distribution_any(equip_df)

                result = (
                    "📊 <b>Общее по республике</b>\n\n"
                    f"🚗 <b>Автотранспорт:</b> {auto_qty} ед.\n"
                    f"⚙️ <b>Прочая техника и оборудование:</b> {equip_qty} ед.\n"
                    f"📊 <b>Всего:</b> {total} ед.\n\n"
                    f"📈 <b>Состояния — автотранспорт:</b>\n"
                    f"{fmt_status_distribution(status_auto)}\n\n"
                    f"📈 <b>Состояния — прочая техника и оборудование:</b>\n"
                    f"{fmt_status_distribution(status_equip)}\n\n"
                )
                result += "📭 Данных нет" if types_df.empty else fmt_table(types_df, COL_TYPE)

                kb = InlineKeyboardBuilder()
                kb.button(text="↩️ Назад к областям", callback_data="back_to_regions")
                kb.adjust(1)
                await safe_edit_message(c.message, result, reply_markup=kb.as_markup())
                return

            # Конкретная область
            df_region = await get_df_async(region, force_refresh=False)
            if df_region.empty:
                await c.message.edit_text("❌ Нет данных для выбранной области")
                return

            auto_qty, equip_qty, total = summarize_region_categories(df_region)

            # Разделяем по категориям для статусов
            types_series = df_region["type_normalized"].astype(str)
            mask_equipment = types_series.apply(is_equipment_type)
            auto_df_region = df_region[~mask_equipment].copy()
            equip_df_region = df_region[mask_equipment].copy()

            status_auto_region = get_status_distribution_any(auto_df_region)
            status_equip_region = get_status_distribution_any(equip_df_region)

            types_df = all_types_summary(df_region)
            if types_df.empty:
                types_block = "\n📋 <b>Типы техники в области:</b>\n📭 Данных о типах техники нет\n"
            else:
                types_block = "\n📋 <b>Типы техники в области:</b>\n" + fmt_table(types_df, COL_TYPE) + "\n"

            text = (
                f"📍 <b>Область:</b> {esc(region)}\n\n"
                f"🚗 <b>Автотранспорт:</b> {auto_qty} ед.\n"
                f"⚙️ <b>Прочая техника и оборудование:</b> {equip_qty} ед.\n"
                f"📊 <b>Всего:</b> {total} ед.\n\n"
                f"📈 <b>Состояния — автотранспорт:</b>\n"
                f"{fmt_status_distribution(status_auto_region)}\n\n"
                f"📈 <b>Состояния — прочая техника и оборудование:</b>\n"
                f"{fmt_status_distribution(status_equip_region)}"
                f"{types_block}\n"
                f"🏙️ <b>Выбери район для детализации</b>"
            )

            kb = create_districts_keyboard(df_region, region)
            await safe_edit_message(c.message, text, reply_markup=kb)

        except Exception as e:
            logger.error(f"❌ Ошибка в show_region_summary: {e}")
            await c.message.edit_text("❌ Ошибка при загрузке данных")

    @dp.message(F.text == "📊 Общее по республике")
    async def types_all(m: types.Message):
        try:
            loading_msg = await m.answer("🔄 Загружаю данные...")
            df = await get_df_async(None, force_refresh=False)
            types_df = all_types_summary(df)

            auto_qty, equip_qty, total = summarize_republic_categories(df)

            if df.empty or "type_normalized" not in df.columns:
                auto_df = pd.DataFrame(columns=df.columns)
                equip_df = pd.DataFrame(columns=df.columns)
            else:
                types = df["type_normalized"].astype(str)
                mask_equipment = types.apply(is_equipment_type)
                auto_df = df[~mask_equipment].copy()
                equip_df = df[mask_equipment].copy()

            status_auto = get_status_distribution_any(auto_df)
            status_equip = get_status_distribution_any(equip_df)

            result = (
                "📊 <b>Общее по республике</b>\n\n"
                f"🚗 <b>Автотранспорт:</b> {auto_qty} ед.\n"
                f"⚙️ <b>Прочая техника и оборудование:</b> {equip_qty} ед.\n"
                f"📊 <b>Всего:</b> {total} ед.\n\n"
                f"📈 <b>Состояния — автотранспорт:</b>\n"
                f"{fmt_status_distribution(status_auto)}\n\n"
                f"📈 <b>Состояния — прочая техника и оборудование:</b>\n"
                f"{fmt_status_distribution(status_equip)}\n\n"
            )

            result += "📭 Данных нет" if types_df.empty else fmt_table(types_df, COL_TYPE)

            await loading_msg.delete()
            await m.answer(result)
        except Exception as e:
            logger.error(f"❌ Ошибка в types_all: {e}")
            await m.answer("❌ Ошибка при загрузке данных")

    @dp.message(F.text == "🔎 Посчитать по типу")
    async def ask_type(m: types.Message):
        try:
            loading_msg = await m.answer("🔄 Загружаю список типов техники...")
            df_all = await get_df_async(None, force_refresh=False)
            types_df = all_types_summary(df_all)
            if types_df.empty:
                await loading_msg.delete()
                await m.answer("❌ Нет данных о технике")
                return
            kb, valid_types = create_types_keyboard(types_df)
            if valid_types == 0:
                await loading_msg.delete()
                await m.answer("❌ Не найдено валидных типов техники")
                return
            await loading_msg.delete()
            await m.answer("🔎 Выбери тип техники:", reply_markup=kb)
        except Exception as e:
            logger.error(f"❌ Ошибка в ask_type: {e}")
            await m.answer("❌ Ошибка при загрузке типов техники")

    @dp.message(F.text == "📡 GPS-учёт по областям")
    async def trackers_by_regions(m: types.Message):
        try:
            loading_msg = await m.answer("📡 Считаю технику с GPS по областям...")
            df_all = await get_df_async(None, force_refresh=False)
            if df_all.empty:
                await loading_msg.delete()
                await m.answer("📭 Данных нет")
                return

            df_tr = count_trackers_by_region(df_all)
            total_trackers = int(df_tr["qty"].sum()) if not df_tr.empty else 0

            lines: List[str] = []
            for region in REGION_SHEETS.keys():
                if df_tr.empty:
                    qty = 0
                else:
                    qty = int(df_tr.loc[df_tr["region_name"] == region, "qty"].sum())
                lines.append(f"• {esc(region)} — {qty} ед.")

            text = (
                f"📡 <b>GPS-учёт техники по областям</b>\n"
                f"Всего техники с GPS: <b>{total_trackers}</b> ед.\n\n"
                + "\n".join(lines)
            )

            await loading_msg.delete()
            await m.answer(text)
        except Exception as e:
            logger.error(f"❌ Ошибка в trackers_by_regions: {e}")
            await m.answer("❌ Ошибка при подсчёте GPS-учёта по областям")

    # --- роутер сокращённых callback_data (цифровые ключи put_cb) ---
    @dp.callback_query(F.data.regexp(r"^\d+$"))
    async def router_short_cb(c: types.CallbackQuery):
        payload = get_cb(c.data) or c.data

        if payload.startswith("count_type:"):
            try:
                await c.answer("📍 Загружаю распределение по областям...")
                tech_type = payload.split(":", 1)[1]
                df_all = await get_df_async(None, force_refresh=False)
                region_df = count_type_per_region(df_all, tech_type)
                if region_df.empty:
                    await c.message.edit_text(
                        f"❌ Тип «{esc(tech_type)}» не найден в данных\n\n"
                        f"<i>Нормализованное название: '{esc(normalize_tech_type(tech_type) or '')}'</i>\n"
                        f"<i>Попробуйте выбрать другой тип</i>"
                    )
                    return

                kb = InlineKeyboardBuilder()
                for _, row in region_df.iterrows():
                    region = str(row["region_name"])
                    count = int(row["qty"])
                    sub_payload = f"type_region:{tech_type}|{region}"
                    cbid = put_cb(sub_payload)
                    kb.button(text=f"{region} ({count} ед.)", callback_data=cbid)

                kb.button(text="↩️ Назад к типам", callback_data="back_to_types")
                kb.adjust(2, 1)

                total = int(region_df["qty"].sum())
                result = f"📊 <b>{esc(tech_type)}</b> — всего {total} ед.\nВыбери область для деталей:"
                await safe_edit_message(c.message, result, reply_markup=kb.as_markup())
            except Exception as e:
                logger.error(f"❌ Ошибка в handle count_type: {e}")
                await c.message.edit_text("❌ Ошибка при обработке запроса")
            return

        if payload.startswith("type_region:"):
            try:
                await c.answer("🔍 Загружаю детальную информацию...")
                data = payload.split(":", 1)[1]
                tech_type, region = data.split("|")
                df_all = await get_df_async(None, force_refresh=False)

                status_distribution = get_status_distribution_for_type_region(df_all, tech_type, region)
                city_status_distribution = get_detailed_city_status(df_all, tech_type, region)

                normalized_type = normalize_tech_type(tech_type)
                total_mask = (df_all["region_name"] == region) & (df_all["type_normalized"] == normalized_type)
                total_count = int(df_all.loc[total_mask, "qty"].sum())

                result = (
                    f"🔍 <b>Детали по {esc(tech_type)}</b>\n"
                    f"📍 <b>Регион:</b> {esc(region)}\n"
                    f"📊 <b>Всего в регионе:</b> {total_count} ед.\n\n"
                    f"📈 <b>Итого по состояниям:</b> {summarize_overall_status(status_distribution)}\n\n"
                    f"🏙️ <b>Детали по городам/районам</b>\n"
                    f"(🟩 Ярокли | 🟨 Таъмирталаб | 🟥 Яроксиз):\n\n"
                )
                if city_status_distribution:
                    result += fmt_detailed_city_status(city_status_distribution)
                else:
                    result += "📭 Нет детальных данных по городам"

                kb = InlineKeyboardBuilder()
                back_id = put_cb(f"count_type:{tech_type}")
                kb.button(text="↩️ Назад к регионам", callback_data=back_id)
                kb.button(text="🏠 Главное меню", callback_data="main_menu")
                kb.adjust(1)

                await safe_edit_message(c.message, result, reply_markup=kb.as_markup())
            except Exception as e:
                logger.error(f"❌ Ошибка в handle type_region: {e}")
                await c.message.edit_text("❌ Ошибка при загрузке деталей")
            return

        if payload.startswith("district:"):
            try:
                await c.answer("🏙️ Загружаю данные по району...")

                data = payload.split(":", 1)[1]
                region, district = data.split("|", 1)

                df_region = await get_df_async(region, force_refresh=False)
                if df_region.empty:
                    await c.message.answer(
                        f"📭 Нет данных для области <b>{esc(region)}</b>",
                    )
                    return

                sub = df_region[df_region["city_district"] == district]
                if sub.empty:
                    await c.message.answer(
                        f"📭 Нет данных для района <b>{esc(district)}</b> в области {esc(region)}"
                    )
                    return

                auto_qty, equip_qty, total = summarize_district_categories(df_region, district)
                status_df = get_status_distribution_any(sub)

                types_block = (
                    "\n📋 <b>Типы техники в районе (с состояниями):</b>\n"
                    f"{fmt_types_with_statuses(sub)}"
                )

                text = (
                    f"🏙️ <b>Район:</b> {esc(district)}\n"
                    f"📍 <b>Область:</b> {esc(region)}\n\n"
                    f"🚗 <b>Автотранспорт:</b> {auto_qty} ед.\n"
                    f"⚙️ <b>Прочая техника и оборудование:</b> {equip_qty} ед.\n"
                    f"📊 <b>Всего:</b> {total} ед.\n\n"
                    f"📈 <b>Состояния (всего по району):</b>\n"
                    f"{fmt_status_distribution(status_df)}"
                    f"{types_block}"
                )

                kb = InlineKeyboardBuilder()
                kb.button(text="↩️ Назад к области", callback_data=f"region:{region}")
                kb.button(text="🏠 Главное меню", callback_data="main_menu")
                kb.adjust(1)
                await c.message.answer(text, reply_markup=kb.as_markup())
            except Exception as e:
                logger.error(f"❌ Ошибка в district-handler: {e}")
                await c.message.answer("❌ Ошибка при загрузке данных по району")
            return

        await c.answer("Неизвестное действие", show_alert=False)

    @dp.callback_query(F.data == "main_menu")
    async def handle_main_menu(c: types.CallbackQuery):
        await c.answer("🏠 Возвращаюсь в главное меню...")
        await start_cmd(c.message)

    @dp.callback_query(F.data == "back_to_regions")
    async def handle_back_to_regions(c: types.CallbackQuery):
        await c.answer("📍 Возвращаюсь к списку областей...")
        await choose_region(c.message)

    @dp.callback_query(F.data == "back_to_types")
    async def handle_back_to_types(c: types.CallbackQuery):
        await c.answer("🔎 Возвращаюсь к списку типов...")
        await ask_type(c.message)

    @dp.message(F.text == "🔄 Обновить кеш")
    async def clear_cache_cmd(m: types.Message):
        global CACHE
        CACHE.clear()
        await m.answer("🔄 Кеш очищен! Данные обновятся при следующем запросе.")

    @dp.message(Command("clear_cache"))
    async def clear_cache_command(m: types.Message):
        global CACHE
        CACHE.clear()
        await m.answer("🔄 Кеш очищен! Данные обновятся при следующем запросе.")

    @dp.message(Command("stats"))
    async def stats_cmd(m: types.Message):
        try:
            df_all = await get_df_async(None, force_refresh=False)
            total_records = len(df_all)
            total_qty = int(df_all["qty"].sum()) if not df_all.empty else 0
            cache_size = len(CACHE)

            types_df = all_types_summary(df_all)
            unique_types = len(types_df) if not types_df.empty else 0

            trackers_total = 0
            if not df_all.empty and "has_tracker" in df_all.columns:
                try:
                    trackers_total = int(df_all.loc[df_all["has_tracker"], "qty"].sum())
                except Exception as err:
                    logger.error(f"❌ Ошибка при подсчёте GPS в stats_cmd: {err}")

            status_stats = ""
            if "status_normalized" in df_all.columns and not df_all.empty:
                status_df = df_all.groupby("status_normalized")["qty"].sum().sort_values(ascending=False)
                status_stats = "\n".join([f"• {esc(st)}: {int(q)} ед." for st, q in status_df.items()])

            next_update = "неизвестно"
            if "ALL" in CACHE:
                _, exp = CACHE["ALL"]
                mins = max(0, int((exp - datetime.now()).total_seconds() // 60))
                next_update = f"{mins} мин"

            stats_text = (
                f"📈 <b>Статистика бота</b>\n\n"
                f"• Всего записей: {total_records}\n"
                f"• Общее количество техники: {total_qty} ед.\n"
                f"• Техники с GPS: {trackers_total} ед.\n"
                f"• Уникальных типов техники: {unique_types}\n"
                f"• Размер кеша: {cache_size} элементов\n"
                f"• Следующее обновление кеша: {esc(next_update)}\n\n"
            )
            if status_stats:
                stats_text += f"<b>📊 Состояние техники (всего):</b>\n{status_stats}"

            await m.answer(stats_text)
        except Exception as e:
            logger.error(f"❌ Ошибка в stats_cmd: {e}")
            await m.answer("❌ Ошибка при получении статистики")

    @dp.message(Command("help"))
    async def help_command(m: types.Message):
        await help_cmd(m)

    @dp.message()
    async def handle_other_messages(m: types.Message):
        await m.answer(
            "🤖 Используйте кнопки меню или команды:\n"
            "/start — главное меню\n"
            "/help — справка",
            reply_markup=main_menu_kb()
        )

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("🚀 Бот запущен: async I/O, области→районы, статусы, категории техники, GPS-учёт")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    except Exception as e:
        logger.error(f"❌ Ошибка запуска бота: {e}")
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
