import os, re, asyncio, json, time, hashlib
import httpx
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Text, func, or_, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ========== НАСТРОЙКИ ==========
API_KEY = os.getenv("API_KEY", "")
SUPABASE_URL = os.getenv("DATABASE_URL", "").replace("postgres://", "postgresql://", 1)
XATA_URL = os.getenv("XATA_DATABASE_URL", "").replace("postgres://", "postgresql://", 1)

JITLER_URL = "https://api.jitler.top"
JITLER_KEYS = [k.strip() for k in os.getenv("JITLER_KEYS", "").split(",") if k.strip()]
PANT_URL = "https://pant-api.cc.cd"
PANT_TOKEN = os.getenv("PANT_TOKEN", "")
CACHE_TTL = 60 * 60 * 24


def _make_engine(url):
    if not url:
        return None
    return create_engine(url, pool_pre_ping=True, pool_size=3, max_overflow=5,
                         pool_recycle=300, connect_args={"connect_timeout": 15})


engine_supabase = _make_engine(SUPABASE_URL)
engine_xata = _make_engine(XATA_URL)
SessionSupabase = sessionmaker(bind=engine_supabase) if engine_supabase else None
SessionXata = sessionmaker(bind=engine_xata) if engine_xata else None

print(f"[init] Supabase: {'OK' if engine_supabase else 'OFF'}")
print(f"[init] Xata:     {'OK' if engine_xata else 'OFF'}")

Base = declarative_base()


class Person(Base):
    __tablename__ = "persons"
    id = Column(Integer, primary_key=True, index=True)
    uniq = Column(String, unique=True, index=True)
    phone = Column(String, index=True)
    fio = Column(String, index=True)
    dob = Column(String)
    operator = Column(String)
    region = Column(String)
    country = Column(String)
    phonebooks = Column(String)
    passport = Column(String)
    snils = Column(String)
    inn = Column(String)
    address = Column(String)
    work = Column(String)
    banks = Column(String)
    email = Column(String)
    telegram = Column(String, index=True)
    vk = Column(String)
    ok = Column(String)
    instagram = Column(String)
    tiktok = Column(String)
    max_link = Column(String)
    whatsapp = Column(String)
    car = Column(String)
    extra = Column(String)
    source = Column(String)


class JitlerCache(Base):
    __tablename__ = "jitler_cache"
    id = Column(Integer, primary_key=True)
    query = Column(String, index=True, unique=True)
    type = Column(String)
    response = Column(Text)
    created = Column(Integer)


class PantCache(Base):
    __tablename__ = "pant_cache"
    id = Column(Integer, primary_key=True)
    query = Column(String, index=True, unique=True)
    type = Column(String)
    response = Column(Text)
    created = Column(Integer)


class PersonCreate(BaseModel):
    uniq: Optional[str] = None
    phone: Optional[str] = None
    fio: Optional[str] = None
    dob: Optional[str] = None
    operator: Optional[str] = None
    region: Optional[str] = None
    country: Optional[str] = None
    phonebooks: Optional[str] = None
    passport: Optional[str] = None
    snils: Optional[str] = None
    inn: Optional[str] = None
    address: Optional[str] = None
    work: Optional[str] = None
    banks: Optional[str] = None
    email: Optional[str] = None
    telegram: Optional[str] = None
    vk: Optional[str] = None
    ok: Optional[str] = None
    instagram: Optional[str] = None
    tiktok: Optional[str] = None
    max_link: Optional[str] = None
    whatsapp: Optional[str] = None
    car: Optional[str] = None
    extra: Optional[str] = None
    source: Optional[str] = None


class PersonResponse(PersonCreate):
    id: int
    class Config:
        from_attributes = True


# ========== УТИЛИТЫ ==========
def get_db():
    if SessionSupabase is None:
        yield None
        return
    db = SessionSupabase()
    try:
        yield db
    finally:
        db.close()


def check_api_key(api_key: str = Query(...)):
    if not API_KEY or api_key != API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")
    return api_key


def normalize_phone(s):
    if not s:
        return ""
    d = re.sub(r"\D", "", str(s))
    if len(d) == 11 and d.startswith("8"):
        d = "7" + d[1:]
    if len(d) == 10:
        d = "7" + d
    return d


def tg_format(q):
    q = q.strip()
    if not q:
        return q
    if re.fullmatch(r"\d+", q):
        return q
    if q.startswith("@"):
        return q
    return "@" + q


OPERATOR_CANON = {
    "билайн": "Билайн", "beeline": "Билайн",
    "мтс": "МТС", "mts": "МТС",
    "мегафон": "Мегафон", "megafon": "Мегафон",
    "tele2": "Tele2", "теле2": "Tele2",
    "т2": "Т2", "t2": "Т2",
    "yota": "Yota", "йота": "Yota",
    "сбер": "Сбербанк", "sber": "Сбербанк",
    "альфа": "Альфа-Банк", "alfa": "Альфа-Банк",
    "втб": "ВТБ", "vtb": "ВТБ",
    "тинькофф": "Т-Банк", "tinkoff": "Т-Банк",
}


def canon_operator(op):
    if not op:
        return ""
    s = str(op).lower().strip()
    for k, v in OPERATOR_CANON.items():
        if k in s:
            return v
    return op


DEF_OPERATOR = {
    "903": "Билайн", "905": "Билайн", "906": "Билайн", "909": "Билайн",
    "960": "Билайн", "961": "Билайн", "962": "Билайн", "963": "Билайн",
    "964": "Билайн", "965": "Билайн", "966": "Билайн", "967": "Билайн",
    "968": "Билайн", "969": "Билайн",
    "910": "МТС", "911": "МТС", "912": "МТС", "913": "МТС", "914": "МТС",
    "915": "МТС", "916": "МТС", "917": "МТС", "918": "МТС", "919": "МТС",
    "980": "МТС", "981": "МТС", "982": "МТС", "983": "МТС", "984": "МТС",
    "985": "МТС", "986": "МТС", "987": "МТС", "988": "МТС", "989": "МТС",
    "920": "Мегафон", "921": "Мегафон", "922": "Мегафон", "923": "Мегафон",
    "924": "Мегафон", "925": "Мегафон", "926": "Мегафон", "927": "Мегафон",
    "928": "Мегафон", "929": "Мегафон", "930": "Мегафон", "931": "Мегафон",
    "932": "Мегафон", "933": "Мегафон", "934": "Мегафон", "936": "Мегафон",
    "937": "Мегафон", "938": "Мегафон", "939": "Мегафон",
    "901": "Tele2", "902": "Tele2", "904": "Tele2", "908": "Tele2",
    "950": "Tele2", "951": "Tele2", "952": "Tele2", "953": "Tele2",
    "958": "Tele2", "977": "Tele2", "991": "Tele2", "992": "Tele2",
    "993": "Tele2", "994": "Tele2", "995": "Tele2", "996": "Tele2",
    "999": "Tele2",
}


def detect_operator_by_phone(phone):
    p = normalize_phone(phone)
    if len(p) != 11:
        return ""
    return DEF_OPERATOR.get(p[1:4], "")


def detect_operator_from_persons(persons, query_phone=""):
    for d in persons:
        op = canon_operator(d.get("operator") or "")
        if op:
            return op
    if query_phone:
        op = detect_operator_by_phone(query_phone)
        if op:
            return op
    return ""


BRAND_PATTERNS = [
    (["beeline", "билайн"], "Билайн"),
    (["mts", "мтс"], "МТС"),
    (["megafon", "мегафон"], "Мегафон"),
    (["tele2", "теле2"], "Tele2"),
    (["yota", "йота"], "Yota"),
    (["сбер", "sber"], "Сбербанк"),
    (["альфа", "alfa"], "Альфа-Банк"),
    (["втб", "vtb"], "ВТБ"),
    (["тинькофф", "tinkoff", "т-банк"], "Т-Банк"),
    (["русский стандарт"], "Русский Стандарт"),
    (["яндекс еда", "yandex eda", "eda.yandex", "яндекс.еда"], "Яндекс.Еда"),
    (["яндекс такси", "yandex taxi"], "Яндекс.Такси"),
    (["яндекс лавка", "lavka"], "Яндекс.Лавка"),
    (["яндекс", "yandex"], "Яндекс"),
    (["самокат", "samokat"], "Самокат"),
    (["ozon", "озон"], "Ozon"),
    (["wildberries", " wb "], "Wildberries"),
    (["avito", "авито"], "Avito"),
    (["dns", "днс"], "DNS"),
    (["сдэк", "cdek", "sdek"], "СДЭК"),
    (["госуслуги", "gosuslugi"], "Госуслуги"),
    (["гибдд", "гаи"], "ГИБДД"),
    (["2gis", "2гис"], "2GIS"),
    (["zarina"], "Zarina"),
    (["kari"], "Kari"),
    (["вконтакте", "vkontakte", "vk.com"], "ВКонтакте"),
    (["одноклассники", "ok.ru"], "Одноклассники"),
    (["mail.ru"], "Mail.ru"),
    (["gmail"], "Gmail"),
    (["mvideo", "мвидео"], "М.Видео"),
    (["связной", "svyaznoy"], "Связной"),
    (["почта россии", "russianpost"], "Почта России"),
    (["steam"], "Steam"),
    (["telegram"], "Telegram"),
]


def _contains_any(haystack, needles):
    if not haystack:
        return False
    h = str(haystack).lower()
    return any(n in h for n in needles)


def classify_leak(d):
    src = (d.get("source") or "").strip()
    if src:
        for needles, name in BRAND_PATTERNS:
            if _contains_any(src, needles):
                return name
        return src.rsplit(".", 1)[0][:40]

    for field in ("extra", "address", "work", "banks"):
        val = d.get(field)
        if val:
            for needles, name in BRAND_PATTERNS:
                if _contains_any(val, needles):
                    return name

    has = lambda f: bool(d.get(f))

    if has("car"): return "ГИБДД / Авто"
    if has("banks"): return "Банковская утечка"
    if has("passport") and (has("snils") or has("inn")): return "Госуслуги"
    if has("snils") or has("inn"): return "ФНС"
    if has("passport"): return "Паспортная база"
    if has("address") and (has("fio") or has("phone")): return "Доставка"
    if has("address"): return "Адресная утечка"
    if has("phonebooks"): return "Телефонная книга"
    if has("telegram") or has("vk") or has("ok") or has("instagram") or has("tiktok"): return "Соцсети"
    if has("email"): return "Почтовая утечка"
    if has("operator"): return "Оператор связи"
    return "Утечка данных"


def _split_list(val):
    """'a, b, c' → ['a','b','c'] с очисткой."""
    if not val:
        return []
    out = []
    for x in re.split(r'[,;]\s*', str(val)):
        x = x.strip()
        if x and x not in out:
            out.append(x)
    return out


def _parse_banks(val):
    """'Альфа-Банк (Фиал Р. Г.), Банк Русский Стандарт (Рима Ф. М.)' → список."""
    if not val:
        return []
    parts = re.split(r'[,;]\s*(?=[А-ЯA-Z])', str(val))
    out = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        m = re.match(r'^(.+?)\s*\((.+)\)\s*$', p)
        if m:
            out.append({"bank": m.group(1).strip(), "fio": m.group(2).strip()})
        else:
            out.append({"bank": p, "fio": None})
    return out


def _parse_work(val):
    """'Южуралникель, обжигальщик' → [{'company': 'Южуралникель', 'position': 'обжигальщик'}]"""
    if not val:
        return []
    out = []
    for p in re.split(r'[;]\s*', str(val)):
        p = p.strip()
        if not p:
            continue
        if ',' in p:
            parts = [x.strip() for x in p.split(',', 1)]
            out.append({"company": parts[0], "position": parts[1] if len(parts) > 1 else None})
        else:
            out.append({"company": p, "position": None})
    return out


# ========== СТРУКТУРИРОВАННЫЙ ОТВЕТ ==========
def build_structured(persons, query_phone="", operator=""):
    """
    Возвращает структурированный ответ:
    - summary: краткая сводка (одна запись)
    - leaks: список утечек/источников
    - banks, work, phonebooks, possible_names: доп. разделы
    - socials: соцсети
    - documents: паспорт/снилс/инн
    - emails: почты
    Пустые разделы не включаются.
    """
    if not persons:
        return {}

    # Собираем уникальные значения по всем записям
    fios, dobs, phones, regions, countries, operators = [], [], [], [], [], []
    emails, telegrams, vks, oks, instagrams, tiktoks = [], [], [], [], [], []
    phonebooks_set = []
    banks_all = []
    work_all = []
    passports, snilses, innes = [], [], []
    addresses = []
    cars = []

    def _add(lst, val):
        if not val:
            return
        for x in _split_list(val):
            if x and x not in lst:
                lst.append(x)

    for d in persons:
        _add(fios, d.get("fio"))
        _add(dobs, d.get("dob"))
        _add(phones, d.get("phone"))
        _add(regions, d.get("region"))
        _add(countries, d.get("country"))
        _add(operators, d.get("operator"))
        _add(emails, d.get("email"))
        _add(telegrams, d.get("telegram"))
        _add(vks, d.get("vk"))
        _add(oks, d.get("ok"))
        _add(instagrams, d.get("instagram"))
        _add(tiktoks, d.get("tiktok"))
        _add(passports, d.get("passport"))
        _add(snilses, d.get("snils"))
        _add(innes, d.get("inn"))
        _add(addresses, d.get("address"))
        _add(cars, d.get("car"))

        # телефонная книга — особый случай (длинный список имён)
        if d.get("phonebooks"):
            for x in _split_list(d.get("phonebooks")):
                if x not in phonebooks_set:
                    phonebooks_set.append(x)

        # банки — парсим
        for b in _parse_banks(d.get("banks")):
            if b not in banks_all:
                banks_all.append(b)

        # работа — парсим
        for w in _parse_work(d.get("work")):
            if w not in work_all:
                work_all.append(w)

    # Определяем оператора
    if not operator:
        operator = operators[0] if operators else detect_operator_by_phone(query_phone)

    # ===== SUMMARY =====
    summary = {}
    if phones: summary["phone"] = phones[0]
    if operator: summary["operator"] = operator
    if regions: summary["region"] = regions[0]
    if countries: summary["country"] = countries[0]
    if fios: summary["fio"] = fios
    if dobs: summary["dob"] = dobs

    result = {
        "query": query_phone,
        "type": "phone",
        "operator": operator or "не определён",
    }
    if summary:
        result["summary"] = summary

    # ===== LEAKS (только те, что имеют данные) =====
    leak_groups = {}
    for d in persons:
        # фильтр по оператору (мягкий)
        if operator:
            d_op = canon_operator(d.get("operator") or "")
            if d_op and d_op != operator:
                continue

        src = classify_leak(d)
        if src not in leak_groups:
            leak_groups[src] = {"source": src}
            for f in ("fio", "phone", "email", "address", "passport",
                      "snils", "inn", "dob", "car"):
                leak_groups[src][f] = None

        g = leak_groups[src]
        for f in ("fio", "phone", "email", "address", "passport",
                  "snils", "inn", "dob", "car"):
            v = d.get(f)
            if not v:
                continue
            v = str(v).strip()
            if not v:
                continue
            if g[f] is None:
                g[f] = v
            elif v not in g[f]:
                g[f] = g[f] + ", " + v

    leaks = []
    for g in leak_groups.values():
        # не включаем блок, если у него нет ничего кроме source
        has = any(g[f] for f in ("fio", "phone", "email", "address",
                                  "passport", "snils", "inn", "dob", "car"))
        if not has:
            continue
        row = {"source": g["source"]}
        for f in ("fio", "phone", "email", "address", "passport",
                  "snils", "inn", "dob", "car"):
            if g[f]:
                row[f] = g[f]
        leaks.append(row)

    if leaks:
        result["leaks"] = leaks

    # ===== BANKS =====
    if banks_all:
        result["banks"] = banks_all

    # ===== WORK =====
    if work_all:
        result["work"] = work_all

    # ===== PHONEBOOKS =====
    if phonebooks_set:
        result["phonebooks"] = phonebooks_set

    # ===== DOCUMENTS =====
    docs = {}
    if passports: docs["passport"] = passports
    if snilses: docs["snils"] = snilses
    if innes: docs["inn"] = innes
    if docs:
        result["documents"] = docs

    # ===== SOCIALS =====
    socials = {}
    if telegrams: socials["telegram"] = telegrams
    if vks: socials["vk"] = vks
    if oks: socials["ok"] = oks
    if instagrams: socials["instagram"] = instagrams
    if tiktoks: socials["tiktok"] = tiktoks
    if socials:
        result["socials"] = socials

    # ===== EMAILS =====
    if emails:
        result["emails"] = emails

    # ===== ADDRESSES =====
    if addresses:
        result["addresses"] = addresses

    # ===== CARS =====
    if cars:
        result["cars"] = cars

    # ===== MESSENGERS =====
    phone_for_msg = phones[0] if phones else query_phone
    if phone_for_msg:
        p = normalize_phone(phone_for_msg)
        if len(p) == 11:
            result["messengers"] = {
                "telegram": f"https://t.me/+{p}",
                "whatsapp": f"https://wa.me/{p}",
                "viber": f"viber://chat?number=%2B{p}",
                "max": f"https://max.ru/+{p}",
            }

    return result


def build_messengers(phone):
    if not phone:
        return {}
    p = normalize_phone(phone)
    if len(p) != 11:
        return {}
    return {
        "telegram": f"https://t.me/+{p}",
        "whatsapp": f"https://wa.me/{p}",
        "viber": f"viber://chat?number=%2B{p}",
        "max": f"https://max.ru/+{p}",
    }


def _ok(d):
    return isinstance(d, dict) and d and "error" not in d


# ========== ДВОЙНОЙ ПОИСК ==========
async def dual_query(query_fn, limit=50):
    def _do(SF, name):
        if SF is None:
            return []
        s = SF()
        try:
            return [{**PersonResponse.model_validate(p).model_dump(), "_source": name} for p in query_fn(s)]
        finally:
            s.close()

    tasks = []
    if SessionSupabase is not None:
        tasks.append(asyncio.to_thread(_do, SessionSupabase, "supabase"))
    if SessionXata is not None:
        tasks.append(asyncio.to_thread(_do, SessionXata, "xata"))

    raw = await asyncio.gather(*tasks, return_exceptions=True) if tasks else []

    merged, seen = [], set()
    for res in raw:
        if isinstance(res, Exception):
            continue
        for d in res:
            key = d.get("uniq") or f"{d.get('phone')}|{d.get('fio')}"
            if key in seen:
                continue
            seen.add(key)
            merged.append(d)
            if len(merged) >= limit:
                return merged
    return merged


# ========== JITLER ==========
async def jitler_request(type_, query, page=1):
    if not JITLER_KEYS:
        return None
    async with httpx.AsyncClient(timeout=60) as client:
        for key in JITLER_KEYS:
            try:
                r = await client.post(f"{JITLER_URL}/search",
                    json={"type": type_, "query": query, "page": page},
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"})
                if r.status_code == 200:
                    data = r.json()
                    if "id" in data and "response" not in data:
                        for _ in range(30):
                            await asyncio.sleep(2)
                            r2 = await client.get(f"{JITLER_URL}/search/{data['id']}",
                                headers={"Authorization": f"Bearer {key}"})
                            if r2.status_code == 200:
                                return r2.json()
                            if r2.status_code != 501:
                                break
                    return data
            except Exception:
                continue
    return None


async def jitler_cached(db, type_, query, page=1):
    if db is None:
        return await jitler_request(type_, query, page)
    key = f"{type_}:{query.lower()}:{page}"
    cached = db.query(JitlerCache).filter(JitlerCache.query == key).first()
    if cached and (time.time() - cached.created) < CACHE_TTL:
        try:
            return json.loads(cached.response)
        except Exception:
            pass
    result = await jitler_request(type_, query, page)
    if isinstance(result, dict):
        if cached:
            cached.response = json.dumps(result, ensure_ascii=False)
            cached.created = int(time.time())
        else:
            db.add(JitlerCache(query=key, type=type_, response=json.dumps(result, ensure_ascii=False), created=int(time.time())))
        db.commit()
    return result


async def pant_request(endpoint, params):
    if not PANT_TOKEN:
        return None
    params["token"] = PANT_TOKEN
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{PANT_URL}/{endpoint}", params=params)
            if r.status_code != 200:
                return None
            return r.json()
    except Exception:
        return None


async def pant_cached(db, endpoint, query):
    if db is None:
        params = {"phone": query} if endpoint == "search_phone" else {"q": query}
        return await pant_request(endpoint, params)
    key = f"pant:{endpoint}:{query.lower()}"
    cached = db.query(PantCache).filter(PantCache.query == key).first()
    if cached and (time.time() - cached.created) < CACHE_TTL:
        try:
            return json.loads(cached.response)
        except Exception:
            pass
    params = {"phone": query} if endpoint == "search_phone" else {"q": query}
    result = await pant_request(endpoint, params)
    if isinstance(result, dict):
        if cached:
            cached.response = json.dumps(result, ensure_ascii=False)
            cached.created = int(time.time())
        else:
            db.add(PantCache(query=key, type=endpoint, response=json.dumps(result, ensure_ascii=False), created=int(time.time())))
        db.commit()
    return result


# ========== ПРИЛОЖЕНИЕ ==========
app = FastAPI(title="My Base API")

if engine_supabase:
    try:
        Base.metadata.create_all(bind=engine_supabase, checkfirst=True)
    except Exception as e:
        print(f"[create_all] {e}")


def _ensure_schema(engine, label):
    if engine is None:
        return
    try:
        with engine.begin() as conn:
            r = conn.execute(text("SELECT column_name FROM information_schema.columns WHERE table_name='persons'"))
            existing = {row[0] for row in r}
            for col, typ in {"source": "TEXT", "max_link": "TEXT", "whatsapp": "TEXT",
                             "car": "TEXT", "tiktok": "TEXT", "instagram": "TEXT", "ok": "TEXT"}.items():
                if col not in existing:
                    try:
                        conn.execute(text(f"ALTER TABLE persons ADD COLUMN {col} {typ}"))
                        print(f"[migrate] {label}: added {col}")
                    except Exception as e:
                        print(f"[migrate] {label}: {col} - {e}")
    except Exception as e:
        print(f"[migrate] {label}: {e}")


@app.on_event("startup")
async def startup_migrate():
    async def _run():
        try:
            if engine_supabase:
                await asyncio.to_thread(_ensure_schema, engine_supabase, "supabase")
            if engine_xata:
                await asyncio.to_thread(_ensure_schema, engine_xata, "xata")
        except Exception as e:
            print(f"[startup_migrate] {e}")
    asyncio.create_task(_run())


@app.get("/")
def root():
    return {"status": "ok",
            "supabase": engine_supabase is not None,
            "xata": engine_xata is not None}


@app.get("/health")
async def health():
    res = {}
    for name, SF in [("supabase", SessionSupabase), ("xata", SessionXata)]:
        if SF is None:
            res[name] = "off"
            continue
        try:
            def _c():
                s = SF()
                try:
                    s.execute(text("SELECT 1"))
                    return "ok"
                finally:
                    s.close()
            res[name] = await asyncio.to_thread(_c)
        except Exception:
            res[name] = "error"
    return res


# -------- /phone --------
@app.get("/phone")
async def search_phone(q: str, page: int = 1,
                       api_key: str = Depends(check_api_key),
                       db: Session = Depends(get_db)):
    norm = normalize_phone(q)

    def q_fn(s):
        return s.query(Person).filter(Person.phone.like(f"%{norm}%")).limit(50).all()
    local = await dual_query(q_fn, 50)

    if not local:
        def q_fn2(s):
            return s.query(Person).filter(
                func.regexp_replace(Person.phone, '[^0-9]', '', 'g').like(f"%{norm}%")
            ).limit(50).all()
        local = await dual_query(q_fn2, 50)

    operator = detect_operator_from_persons(local, norm)

    result = build_structured(local, norm, operator)

    # external (только успешные)
    external = {}
    j = await jitler_cached(db, "number", norm, page)
    if _ok(j):
        external["jitler"] = j
    p = await pant_cached(db, "search_phone", norm)
    if _ok(p):
        external["pant"] = p
    if external:
        result["external"] = external

    return result


# -------- /telegram --------
@app.get("/telegram")
async def search_telegram(q: str, page: int = 1,
                          api_key: str = Depends(check_api_key),
                          db: Session = Depends(get_db)):
    def q_fn(s):
        return s.query(Person).filter(or_(
            func.lower(Person.telegram).like(f"%{q.lower()}%"),
            func.lower(Person.extra).like(f"%{q.lower()}%"),
        )).limit(50).all()
    local = await dual_query(q_fn, 50)
    operator = detect_operator_from_persons(local)

    result = build_structured(local, "", operator)
    result["query"] = q
    result["type"] = "telegram"

    external = {}
    formatted = tg_format(q)
    sherlock = await jitler_cached(db, "sherlock", formatted, page)
    funstat = await jitler_cached(db, "funstat", formatted, page)
    if _ok(sherlock):
        external.setdefault("jitler", {})["sherlock"] = sherlock
    if _ok(funstat):
        external.setdefault("jitler", {})["funstat"] = funstat
    p = await pant_cached(db, "search", formatted)
    if _ok(p):
        external["pant"] = p
    if external:
        result["external"] = external

    return result


# -------- /fio --------
@app.get("/fio")
async def search_fio(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(func.lower(Person.fio).like(f"%{q.lower()}%")).limit(50).all()
    local = await dual_query(q_fn, 50)
    operator = detect_operator_from_persons(local)
    result = build_structured(local, "", operator)
    result["query"] = q
    result["type"] = "fio"
    return result


# -------- /documents --------
@app.get("/documents")
async def search_documents(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(or_(
            func.lower(Person.passport).like(f"%{q.lower()}%"),
            func.lower(Person.snils).like(f"%{q.lower()}%"),
            func.lower(Person.inn).like(f"%{q.lower()}%"),
        )).limit(50).all()
    local = await dual_query(q_fn, 50)
    operator = detect_operator_from_persons(local)
    result = build_structured(local, "", operator)
    result["query"] = q
    result["type"] = "documents"
    return result


# -------- /address --------
@app.get("/address")
async def search_address(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(func.lower(Person.address).like(f"%{q.lower()}%")).limit(50).all()
    local = await dual_query(q_fn, 50)
    operator = detect_operator_from_persons(local)
    result = build_structured(local, "", operator)
    result["query"] = q
    result["type"] = "address"
    return result


# -------- /extra --------
@app.get("/extra")
async def search_extra(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(func.lower(Person.extra).like(f"%{q.lower()}%")).limit(50).all()
    local = await dual_query(q_fn, 50)
    operator = detect_operator_from_persons(local)
    result = build_structured(local, "", operator)
    result["query"] = q
    result["type"] = "extra"
    return result


# -------- /vks --------
@app.get("/vks")
async def search_vks(q: str, page: int = 1,
                     api_key: str = Depends(check_api_key),
                     db: Session = Depends(get_db)):
    j = await jitler_cached(db, "vks", q, page)
    result = {"query": q, "type": "vks"}
    if _ok(j):
        result["external"] = {"jitler": j}
    return result


# -------- /ppnd --------
@app.get("/ppnd")
async def ppnd_search(q: str, api_key: str = Depends(check_api_key)):
    parts = [p.strip().lower() for p in q.replace(" ", "_").split("_") if p.strip()]
    if not parts:
        raise HTTPException(400, "Пустой запрос")

    def q_fn(s):
        conds = []
        for part in parts:
            for col in (Person.fio, Person.address, Person.region,
                        Person.country, Person.dob, Person.extra):
                conds.append(func.lower(col).like(f"%{part}%"))
        return s.query(Person).filter(or_(*conds)).limit(300).all()

    rows = await dual_query(q_fn, 300)
    results = []
    for d in rows:
        text = " ".join([str(d.get(k) or "") for k in
                         ("fio", "address", "region", "country", "dob", "extra")]).lower()
        if all(p in text for p in parts):
            results.append(d)
            if len(results) >= 100:
                break
    operator = detect_operator_from_persons(results)
    result = build_structured(results, "", operator)
    result["query"] = q
    result["type"] = "ppnd"
    return result


# -------- /phonebooks --------
@app.get("/phonebooks")
async def search_phonebooks(q: str, limit: int = 50,
                            api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(func.lower(Person.phonebooks).like(f"%{q.lower()}%")).limit(limit).all()
    m = await dual_query(q_fn, limit)
    result = build_structured(m, "", "")
    result["query"] = q
    result["type"] = "phonebooks"
    return result


NICKNAME_SITES = {
    "Telegram": "https://t.me/{u}", "VK": "https://vk.com/{u}", "OK": "https://ok.ru/{u}",
    "GitHub": "https://github.com/{u}", "Yandex": "https://yandex.ru/search/?text={u}",
    "Doxbin": "https://doxbin.org/user/{u}", "TikTok": "https://www.tiktok.com/@{u}",
    "Roblox": "https://www.roblox.com/search/users?keyword={u}", "Twitch": "https://m.twitch.tv/{u}",
    "YouTube": "https://www.youtube.com/{u}", "Facebook": "https://www.facebook.com/{u}",
    "Steam": "https://steamcommunity.com/id/{u}", "Я.Музыка": "https://music.yandex.ru/users/{u}",
    "Playerok": "https://playerok.com/profile/{u}", "Xbox": "https://www.xbox.com/profile/{u}",
    "Trashbox": "https://trashbox.ru/users/{u}", "Spotify": "https://open.spotify.com/artist/{u}",
    "Reddit": "https://reddit.com/user/{u}", "Trakt": "https://www.trakt.tv/user/{u}",
    "Baddo": "https://baddo.com/en/{u}", "OkCupid": "https://www.okcupid.com/profile/{u}",
    "Кинопоиск": "https://www.kinopoisk.ru/mykp/user/{u}", "4chan": "https://www.4chan.org/{u}",
    "Minecraft": "https://www.minecraft.net/ru-ru/user/{u}", "CodeChef": "https://www.codechef.com/users/{u}",
    "Linux.org": "https://www.linux.org.ru/{u}/people", "Guns.ru": "https://forum.guns.ru/forummisc/blog/{u}",
    "Apple Discussions": "https://discussions.apple.com/profile/{u}", "Myspace": "https://myspace.com/{u}",
    "Osu": "https://osu.ppy.sh/users/{u}", "Codewars": "https://www.codewars.com/users/{u}",
    "Enjin": "https://www.enjin.com/profile/{u}", "DDNet": "https://ddnet.org/user/{u}",
}


@app.get("/nickname")
def search_nickname(q: str, api_key: str = Depends(check_api_key)):
    return {"query": q, "type": "nickname", "gmail": f"{q}@gmail.com",
            "links": {name: url.format(u=q) for name, url in NICKNAME_SITES.items()}}


@app.get("/stats")
async def db_stats(api_key: str = Depends(check_api_key)):
    def one(SF):
        if SF is None:
            return {"total": 0}
        s = SF()
        try:
            return {"total": s.query(func.count(Person.id)).scalar() or 0}
        finally:
            s.close()
    sb = await asyncio.to_thread(one, SessionSupabase)
    xa = await asyncio.to_thread(one, SessionXata)
    return {"supabase": sb, "xata": xa,
            "combined_total": sb.get("total", 0) + xa.get("total", 0)}


@app.get("/persons")
async def get_all(skip: int = 0, limit: int = 100,
                  api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).offset(skip).limit(limit).all()
    return await dual_query(q_fn, limit)


@app.get("/persons/{phone}")
async def get_by_phone(phone: str, api_key: str = Depends(check_api_key)):
    norm = normalize_phone(phone)
    def q_fn(s):
        return s.query(Person).filter(
            func.regexp_replace(Person.phone, '[^0-9]', '', 'g') == norm
        ).limit(1).all()
    res = await dual_query(q_fn, 1)
    if not res:
        raise HTTPException(status_code=404, detail="Не найдено")
    return res[0]


@app.post("/persons")
async def create_person(person: PersonCreate, api_key: str = Depends(check_api_key)):
    if person.phone:
        person.phone = normalize_phone(person.phone)
    if SessionXata is None:
        raise HTTPException(status_code=503, detail="Xata недоступна")
    s = SessionXata()
    try:
        if not person.uniq:
            person.uniq = "p:" + (person.phone or hashlib.md5(
                f"{person.fio}|{person.dob}".encode()).hexdigest()[:16])
        existing = s.query(Person).filter(Person.uniq == person.uniq).first()
        if existing:
            for k, v in person.model_dump().items():
                if v is not None:
                    setattr(existing, k, v)
            s.commit()
            s.refresh(existing)
            return PersonResponse.model_validate(existing).model_dump()
        db_person = Person(**person.model_dump())
        s.add(db_person)
        s.commit()
        s.refresh(db_person)
        return PersonResponse.model_validate(db_person).model_dump()
    finally:
        s.close()


@app.delete("/persons/{phone}")
async def delete_person(phone: str, api_key: str = Depends(check_api_key)):
    norm = normalize_phone(phone)
    if SessionXata is None:
        raise HTTPException(status_code=503, detail="Xata недоступна")
    s = SessionXata()
    try:
        person = s.query(Person).filter(
            func.regexp_replace(Person.phone, '[^0-9]', '', 'g') == norm
        ).first()
        if not person:
            raise HTTPException(status_code=404, detail="Не найдено")
        s.delete(person)
        s.commit()
        return {"status": "deleted", "phone": norm}
    finally:
        s.close()
