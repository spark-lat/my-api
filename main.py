import os, re, asyncio, json, time, hashlib
import httpx
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Text, func, or_, text
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ========== НАСТРОЙКИ ==========
API_KEY = os.getenv("API_KEY", "")
SUPABASE_URL = os.getenv("DATABASE_URL", "")
XATA_URL = os.getenv("XATA_DATABASE_URL", "")

def _fix_pg_url(u: str) -> str:
    if not u:
        return u
    if u.startswith("postgres://"):
        u = u.replace("postgres://", "postgresql://", 1)
    return u

SUPABASE_URL = _fix_pg_url(SUPABASE_URL)
XATA_URL = _fix_pg_url(XATA_URL)

JITLER_URL = "https://api.jitler.top"
JITLER_KEYS = [k.strip() for k in os.getenv("JITLER_KEYS", "").split(",") if k.strip()]
PANT_URL = "https://pant-api.cc.cd"
PANT_TOKEN = os.getenv("PANT_TOKEN", "")
HTMLWEB_URL = "https://htmlweb.ru/geo/api.php"
CACHE_TTL = 60 * 60 * 24

# ========== ДВИЖКИ ==========
def _make_engine(url: str):
    if not url:
        return None
    return create_engine(
        url, pool_pre_ping=True, pool_size=3, max_overflow=5,
        pool_recycle=300, connect_args={"connect_timeout": 15},
    )

engine_supabase = _make_engine(SUPABASE_URL)
engine_xata = _make_engine(XATA_URL)
SessionSupabase = sessionmaker(bind=engine_supabase) if engine_supabase else None
SessionXata = sessionmaker(bind=engine_xata) if engine_xata else None

print(f"[init] Supabase: {'OK' if engine_supabase else 'OFF'}")
print(f"[init] Xata:     {'OK' if engine_xata else 'OFF'}")

Base = declarative_base()

# ========== МОДЕЛИ ==========
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

# ========== МИГРАЦИЯ ==========
def _ensure_schema(engine, label: str):
    if engine is None:
        return
    try:
        with engine.begin() as conn:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns WHERE table_name = 'persons'"
            ))
            existing = {row[0] for row in result}
            needed = {"source": "TEXT", "max_link": "TEXT", "whatsapp": "TEXT",
                      "car": "TEXT", "tiktok": "TEXT", "instagram": "TEXT", "ok": "TEXT"}
            for col, typ in needed.items():
                if col not in existing:
                    try:
                        conn.execute(text(f"ALTER TABLE persons ADD COLUMN {col} {typ}"))
                        print(f"[migrate] {label}: added {col}")
                    except Exception as e:
                        print(f"[migrate] {label}: {col} - {e}")
            print(f"[migrate] {label}: ok")
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

if engine_supabase:
    try:
        Base.metadata.create_all(bind=engine_supabase, checkfirst=True)
    except Exception:
        pass

# ========== СХЕМЫ ==========
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

def normalize_phone(s: str) -> str:
    if not s:
        return ""
    d = re.sub(r"\D", "", str(s))
    if len(d) == 11 and d.startswith("8"):
        d = "7" + d[1:]
    if len(d) == 10:
        d = "7" + d
    return d

def tg_format(q: str) -> str:
    q = q.strip()
    if not q:
        return q
    if re.fullmatch(r"\d+", q):
        return q
    if q.startswith("@"):
        return q
    return "@" + q

# ========== ОПЕРАТОРЫ ==========
OPERATOR_CANON = {
    "билайн": "Билайн", "beeline": "Билайн",
    "мтс": "МТС", "mts": "МТС",
    "мегафон": "Мегафон", "megafon": "Мегафон",
    "tele2": "Tele2", "теле2": "Tele2",
    "т2": "Т2", "t2": "Т2",
    "yota": "Yota", "йота": "Yota",
    "сбер": "Сбербанк", "sber": "Сбербанк", "сбербанк": "Сбербанк",
    "альфа": "Альфа-Банк", "alfa": "Альфа-Банк",
    "втб": "ВТБ", "vtb": "ВТБ",
    "тинькофф": "Т-Банк", "tinkoff": "Т-Банк",
}

def canon_operator(op: str) -> str:
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

def detect_operator_by_phone(phone: str) -> str:
    p = normalize_phone(phone)
    if len(p) != 11:
        return ""
    return DEF_OPERATOR.get(p[1:4], "")

def detect_operator_from_persons(persons: list, query_phone: str = "") -> str:
    for d in persons:
        op = canon_operator(d.get("operator") or "")
        if op:
            return op
    if query_phone:
        op = detect_operator_by_phone(query_phone)
        if op:
            return op
    return ""

# ========== ОПРЕДЕЛЕНИЕ УТЕЧКИ ==========
# Бренды по подстроке в source/extra/address
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
    (["яндекс", "yandex", "еда", "eda", "лавка", "lavka"], "Яндекс"),
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
    (["почта", "russianpost", "почты россии"], "Почта России"),
    (["steam"], "Steam"),
    (["telegram"], "Telegram"),
]

def _contains_any(haystack: str, needles: list) -> bool:
    if not haystack:
        return False
    h = str(haystack).lower()
    return any(n in h for n in needles)

def classify_leak(d: dict) -> str:
    """
    Возвращает название утечки/источника для записи.
    Приоритет: source → extra/address (бренд) → по полям.
    """
    # 1. source (имя файла)
    src = (d.get("source") or "").strip()
    if src:
        for needles, name in BRAND_PATTERNS:
            if _contains_any(src, needles):
                return name
        # source без бренда — берём как есть, обрезая
        return src.rsplit(".", 1)[0][:40]

    # 2. extra или address — ищем бренд
    for field in ("extra", "address", "work", "banks"):
        val = d.get(field)
        if val:
            for needles, name in BRAND_PATTERNS:
                if _contains_any(val, needles):
                    return name

    # 3. По содержимому
    has = lambda f: bool(d.get(f))

    if has("car"):
        return "ГИБДД / Авто"
    if has("banks"):
        return "Банковская утечка"
    if has("passport") and (has("snils") or has("inn")):
        return "Госуслуги"
    if has("snils") or has("inn"):
        return "ФНС"
    if has("passport"):
        return "Паспортная база"
    if has("address") and (has("fio") or has("phone")):
        return "Доставка / Маркетплейс"
    if has("address"):
        return "Адресная утечка"
    if has("phonebooks"):
        return "Телефонная книга"
    if has("telegram") or has("vk") or has("ok") or has("instagram") or has("tiktok"):
        return "Соцсети"
    if has("email"):
        return "Почтовая утечка"
    if has("operator"):
        return "Оператор связи"
    return "Утечка данных"

# ========== СБОРКА ОБЪЕДИНЁННЫХ ЗАПИСЕЙ ==========
def _extract_first(val):
    """Из строки 'a, b, c' возвращает первое непустое."""
    if not val:
        return None
    s = str(val).strip()
    if not s:
        return None
    return s

def build_united(persons: list, operator: str = "") -> list:
    """
    Возвращает объединённые записи с полем leak.
    Каждая запись = один источник/утечка.
    """
    groups = {}
    for d in persons:
        # Мягкий фильтр по оператору
        if operator:
            d_op = canon_operator(d.get("operator") or "")
            if d_op and d_op != operator:
                continue

        leak = classify_leak(d)
        if leak not in groups:
            groups[leak] = {
                "leak": leak,
                "fio": None, "phone": None, "email": None,
                "dob": None, "address": None,
                "passport": None, "snils": None, "inn": None,
                "banks": None, "car": None, "work": None,
                "telegram": None, "vk": None, "ok": None,
                "instagram": None, "tiktok": None,
                "comment": "—",
            }
        g = groups[leak]

        def _join(field, value):
            if not value:
                return
            v = str(value).strip()
            if not v:
                return
            if g[field] is None:
                g[field] = v
            elif v not in g[field]:
                g[field] = g[field] + ", " + v

        _join("fio", d.get("fio"))
        _join("phone", d.get("phone"))
        _join("email", d.get("email"))
        _join("dob", d.get("dob"))
        _join("address", d.get("address"))
        _join("passport", d.get("passport"))
        _join("snils", d.get("snils"))
        _join("inn", d.get("inn"))
        _join("banks", d.get("banks"))
        _join("car", d.get("car"))
        _join("work", d.get("work"))
        _join("telegram", d.get("telegram"))
        _join("vk", d.get("vk"))
        _join("ok", d.get("ok"))
        _join("instagram", d.get("instagram"))
        _join("tiktok", d.get("tiktok"))

    out = []
    for g in groups.values():
        # Пропускаем пустые (только leak без данных)
        has_data = any(g[f] for f in ("fio", "phone", "email", "address",
                                       "passport", "snils", "inn", "banks",
                                       "car", "work", "telegram", "vk", "ok",
                                       "instagram", "tiktok"))
        if not has_data:
            continue
        out.append({
            "leak":      g["leak"],
            "fio":       g["fio"] or "—",
            "phone":     g["phone"] or "—",
            "email":     g["email"] or "—",
            "dob":       g["dob"] or "—",
            "address":   g["address"] or "—",
            "passport":  g["passport"] or "—",
            "snils":     g["snils"] or "—",
            "inn":       g["inn"] or "—",
            "banks":     g["banks"] or "—",
            "car":       g["car"] or "—",
            "work":      g["work"] or "—",
            "telegram":  g["telegram"] or "—",
            "vk":        g["vk"] or "—",
            "ok":        g["ok"] or "—",
            "instagram": g["instagram"] or "—",
            "tiktok":    g["tiktok"] or "—",
            "comment":   "—",
        })
    return out

# ========== МЕССЕНДЖЕРЫ ==========
def build_messengers(phone: str) -> dict:
    if not phone:
        return {}
    p = normalize_phone(phone)
    if len(p) != 11:
        return {}
    return {
        "telegram": f"https://t.me/+{p}",
        "whatsapp": f"https://wa.me/{p}",
        "viber":    f"viber://chat?number=%2B{p}",
        "max":      f"https://max.ru/+{p}",
    }

# ========== ДВОЙНОЙ ПОИСК ==========
async def dual_query(query_fn, limit: int = 50):
    def _do(SessionFactory, src_name):
        if SessionFactory is None:
            return []
        s = SessionFactory()
        try:
            rows = query_fn(s)
            out = []
            for p in rows:
                d = PersonResponse.model_validate(p).model_dump()
                d["_source"] = src_name
                out.append(d)
            return out
        finally:
            s.close()

    tasks = []
    if SessionSupabase is not None:
        tasks.append(asyncio.to_thread(_do, SessionSupabase, "supabase"))
    if SessionXata is not None:
        tasks.append(asyncio.to_thread(_do, SessionXata, "xata"))

    if not tasks:
        return []

    raw = await asyncio.gather(*tasks, return_exceptions=True)

    merged, seen = [], set()
    for res in raw:
        if isinstance(res, Exception):
            continue  # ошибки глушим
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
async def jitler_request(type_: str, query: str, page: int = 1):
    if not JITLER_KEYS:
        return None
    async with httpx.AsyncClient(timeout=60) as client:
        for key in JITLER_KEYS:
            try:
                r = await client.post(
                    f"{JITLER_URL}/search",
                    json={"type": type_, "query": query, "page": page},
                    headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                )
                if r.status_code == 200:
                    data = r.json()
                    if "id" in data and "response" not in data:
                        task_id = data["id"]
                        for _ in range(30):
                            await asyncio.sleep(2)
                            r2 = await client.get(
                                f"{JITLER_URL}/search/{task_id}",
                                headers={"Authorization": f"Bearer {key}"},
                            )
                            if r2.status_code == 200:
                                return r2.json()
                            if r2.status_code != 501:
                                break
                    return data
            except Exception:
                continue
    return None

async def jitler_cached(db, type_: str, query: str, page: int = 1):
    if db is None:
        return await jitler_request(type_, query, page)
    cache_key = f"{type_}:{query.lower()}:{page}"
    cached = db.query(JitlerCache).filter(JitlerCache.query == cache_key).first()
    if cached and (time.time() - cached.created) < CACHE_TTL:
        try:
            data = json.loads(cached.response)
            if isinstance(data, dict) and "error" not in data:
                return data
            db.delete(cached)
            db.commit()
        except Exception:
            pass
    result = await jitler_request(type_, query, page)
    if isinstance(result, dict) and "error" not in result:
        if cached:
            cached.response = json.dumps(result, ensure_ascii=False)
            cached.created = int(time.time())
        else:
            db.add(JitlerCache(query=cache_key, type=type_,
                response=json.dumps(result, ensure_ascii=False), created=int(time.time())))
        db.commit()
    return result

# ========== PANT ==========
async def pant_request(endpoint: str, params: dict):
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

async def pant_cached(db, endpoint: str, query: str):
    if db is None:
        params = {"phone": query} if endpoint == "search_phone" else {"q": query}
        return await pant_request(endpoint, params)
    cache_key = f"pant:{endpoint}:{query.lower()}"
    cached = db.query(PantCache).filter(PantCache.query == cache_key).first()
    if cached and (time.time() - cached.created) < CACHE_TTL:
        try:
            return json.loads(cached.response)
        except Exception:
            pass
    params = {"phone": query} if endpoint == "search_phone" else {"q": query}
    result = await pant_request(endpoint, params)
    if isinstance(result, dict) and "error" not in result:
        if cached:
            cached.response = json.dumps(result, ensure_ascii=False)
            cached.created = int(time.time())
        else:
            db.add(PantCache(query=cache_key, type=endpoint,
                response=json.dumps(result, ensure_ascii=False), created=int(time.time())))
        db.commit()
    return result

# ========== HTMLWEB ==========
async def htmlweb_request(def_code: str):
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(HTMLWEB_URL, params={"json": "", "telcod": def_code})
            if r.status_code != 200:
                return None
            return r.json()
    except Exception:
        return None

# ========== ПРИЛОЖЕНИЕ ==========
app = FastAPI(title="My Base API")

@app.get("/")
def root():
    return {"status": "ok",
            "supabase": engine_supabase is not None,
            "xata": engine_xata is not None}

@app.get("/health")
async def health():
    result = {}
    for name, SessionFactory in [("supabase", SessionSupabase), ("xata", SessionXata)]:
        if SessionFactory is None:
            result[name] = "off"
            continue
        try:
            def _check():
                s = SessionFactory()
                try:
                    s.execute(text("SELECT 1"))
                    return "ok"
                finally:
                    s.close()
            result[name] = await asyncio.to_thread(_check)
        except Exception as e:
            result[name] = f"error: {str(e)[:100]}"
    return result

# -------- /phone --------
@app.get("/phone")
async def search_phone(q: str, page: int = 1,
                       api_key: str = Depends(check_api_key),
                       db: Session = Depends(get_db)):
    norm = normalize_phone(q)

    def q_fn(s):
        return s.query(Person).filter(Person.phone.like(f"%{norm}%")).limit(50).all()
    local = await dual_query(q_fn, limit=50)

    if not local:
        def q_fn2(s):
            return s.query(Person).filter(
                func.regexp_replace(Person.phone, '[^0-9]', '', 'g').like(f"%{norm}%")
            ).limit(50).all()
        local = await dual_query(q_fn2, limit=50)

    operator = detect_operator_from_persons(local, norm)

    # Фильтр по оператору
    if operator and local:
        filtered = [d for d in local
                    if not canon_operator(d.get("operator") or "")
                    or canon_operator(d.get("operator") or "") == operator]
        if filtered:
            local = filtered

    united = build_united(local, operator)

    # Мессенджеры
    messengers = build_messengers(norm)

    # External (только успешные, без error)
    external = {}
    j = await jitler_cached(db, "number", norm, page)
    if j and isinstance(j, dict) and "error" not in j:
        external["jitler"] = j
    p = await pant_cached(db, "search_phone", norm)
    if p and isinstance(p, dict) and "error" not in p:
        external["pant"] = p
    h = await htmlweb_request("7")
    if h and isinstance(h, dict) and "error" not in h:
        external["htmlweb"] = h

    return {
        "query": q, "type": "phone",
        "operator": operator or "не определён",
        "united": united,
        "messengers": messengers,
        "external": external,
    }

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
    local = await dual_query(q_fn, limit=50)
    operator = detect_operator_from_persons(local)

    united = build_united(local, operator)

    phone_for_msg = ""
    for d in local:
        if d.get("phone"):
            phone_for_msg = d["phone"]
            break

    formatted = tg_format(q)
    external = {}
    sherlock = await jitler_cached(db, "sherlock", formatted, page)
    funstat = await jitler_cached(db, "funstat", formatted, page)
    if sherlock and isinstance(sherlock, dict) and "error" not in sherlock:
        external.setdefault("jitler", {})["sherlock"] = sherlock
    if funstat and isinstance(funstat, dict) and "error" not in funstat:
        external.setdefault("jitler", {})["funstat"] = funstat
    pant = await pant_cached(db, "search", formatted)
    if pant and isinstance(pant, dict) and "error" not in pant:
        external["pant"] = pant

    return {
        "query": q, "type": "telegram",
        "operator": operator or "не определён",
        "united": united,
        "messengers": build_messengers(phone_for_msg),
        "external": external,
    }

# -------- /fio --------
@app.get("/fio")
async def search_fio(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(
            func.lower(Person.fio).like(f"%{q.lower()}%")
        ).limit(50).all()
    local = await dual_query(q_fn, limit=50)
    operator = detect_operator_from_persons(local)
    return {"query": q, "type": "fio",
            "operator": operator or "не определён",
            "united": build_united(local, operator),
            "messengers": {},
            "external": {}}

# -------- /documents --------
@app.get("/documents")
async def search_documents(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(or_(
            func.lower(Person.passport).like(f"%{q.lower()}%"),
            func.lower(Person.snils).like(f"%{q.lower()}%"),
            func.lower(Person.inn).like(f"%{q.lower()}%"),
        )).limit(50).all()
    local = await dual_query(q_fn, limit=50)
    operator = detect_operator_from_persons(local)
    return {"query": q, "type": "documents",
            "operator": operator or "не определён",
            "united": build_united(local, operator),
            "messengers": {},
            "external": {}}

# -------- /address --------
@app.get("/address")
async def search_address(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(
            func.lower(Person.address).like(f"%{q.lower()}%")
        ).limit(50).all()
    local = await dual_query(q_fn, limit=50)
    operator = detect_operator_from_persons(local)
    return {"query": q, "type": "address",
            "operator": operator or "не определён",
            "united": build_united(local, operator),
            "messengers": {},
            "external": {}}

# -------- /extra --------
@app.get("/extra")
async def search_extra(q: str, api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(
            func.lower(Person.extra).like(f"%{q.lower()}%")
        ).limit(50).all()
    local = await dual_query(q_fn, limit=50)
    operator = detect_operator_from_persons(local)
    return {"query": q, "type": "extra",
            "operator": operator or "не определён",
            "united": build_united(local, operator),
            "messengers": {},
            "external": {}}

# -------- /vks --------
@app.get("/vks")
async def search_vks(q: str, page: int = 1,
                     api_key: str = Depends(check_api_key),
                     db: Session = Depends(get_db)):
    j = await jitler_cached(db, "vks", q, page)
    external = {}
    if j and isinstance(j, dict) and "error" not in j:
        external["jitler"] = j
    return {"query": q, "type": "vks",
            "united": [], "messengers": {}, "external": external}

# -------- /ppnd --------
@app.get("/ppnd")
async def ppnd_search(q: str, api_key: str = Depends(check_api_key)):
    parts = [p.strip().lower() for p in q.replace(" ", "_").split("_") if p.strip()]
    if not parts:
        raise HTTPException(400, "Пустой запрос")

    def q_fn(s):
        conditions = []
        for part in parts:
            for col in (Person.fio, Person.address, Person.region,
                        Person.country, Person.dob, Person.extra):
                conditions.append(func.lower(col).like(f"%{part}%"))
        return s.query(Person).filter(or_(*conditions)).limit(300).all()

    rows = await dual_query(q_fn, limit=300)
    results = []
    for d in rows:
        text = " ".join([
            str(d.get("fio") or ""), str(d.get("address") or ""),
            str(d.get("region") or ""), str(d.get("country") or ""),
            str(d.get("dob") or ""), str(d.get("extra") or ""),
        ]).lower()
        if all(part in text for part in parts):
            results.append(d)
            if len(results) >= 100:
                break

    operator = detect_operator_from_persons(results)
    return {"query": q, "found": len(results),
            "operator": operator or "не определён",
            "united": build_united(results, operator),
            "messengers": {}, "external": {}}

# -------- /phonebooks --------
@app.get("/phonebooks")
async def search_phonebooks(q: str, limit: int = 50,
                            api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).filter(
            func.lower(Person.phonebooks).like(f"%{q.lower()}%")
        ).limit(limit).all()
    matches = await dual_query(q_fn, limit=limit)
    return {"query": q, "type": "phonebooks",
            "found": len(matches),
            "united": build_united(matches),
            "messengers": {}, "external": {}}

# -------- /nickname --------
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
    links = {name: url.format(u=q) for name, url in NICKNAME_SITES.items()}
    return {"query": q, "type": "nickname",
            "gmail": f"{q}@gmail.com", "links": links}

# -------- /stats --------
@app.get("/stats")
async def db_stats(api_key: str = Depends(check_api_key)):
    def one(SessionFactory):
        if SessionFactory is None:
            return {"total": 0, "status": "off"}
        s = SessionFactory()
        try:
            total = s.query(func.count(Person.id)).scalar() or 0
            def cnt(field):
                return s.query(func.count(Person.id)).filter(
                    func.coalesce(field, "") != ""
                ).scalar() or 0
            return {"total": total,
                    "with_phone": cnt(Person.phone),
                    "with_fio": cnt(Person.fio),
                    "with_passport": cnt(Person.passport),
                    "with_snils": cnt(Person.snils),
                    "with_inn": cnt(Person.inn),
                    "with_email": cnt(Person.email),
                    "with_telegram": cnt(Person.telegram),
                    "with_vk": cnt(Person.vk),
                    "with_banks": cnt(Person.banks)}
        finally:
            s.close()
    sb = await asyncio.to_thread(one, SessionSupabase)
    xa = await asyncio.to_thread(one, SessionXata)
    return {"supabase": sb, "xata": xa,
            "combined_total": sb.get("total", 0) + xa.get("total", 0)}

# -------- ВСПОМОГАТЕЛЬНЫЕ --------
@app.get("/persons")
async def get_all(skip: int = 0, limit: int = 100,
                  api_key: str = Depends(check_api_key)):
    def q_fn(s):
        return s.query(Person).offset(skip).limit(limit).all()
    return await dual_query(q_fn, limit=limit)

@app.get("/persons/{phone}")
async def get_by_phone(phone: str, api_key: str = Depends(check_api_key)):
    norm = normalize_phone(phone)
    def q_fn(s):
        return s.query(Person).filter(
            func.regexp_replace(Person.phone, '[^0-9]', '', 'g') == norm
        ).limit(1).all()
    res = await dual_query(q_fn, limit=1)
    if not res:
        raise HTTPException(status_code=404, detail="Не найдено")
    return res[0]

@app.post("/persons")
async def create_person(person: PersonCreate,
                        api_key: str = Depends(check_api_key)):
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
