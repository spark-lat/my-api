import os, re, asyncio, json, time
import httpx
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, Text, func, or_
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ========== НАСТРОЙКИ ==========
API_KEY = os.getenv("API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL") or "sqlite:///./mybase.db"
JITLER_URL = "https://api.jitler.top"
JITLER_KEYS = [k.strip() for k in os.getenv("JITLER_KEYS", "").split(",") if k.strip()]
CACHE_TTL = 60 * 60 * 24  # 24 часа

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ========== МОДЕЛИ ==========
class Person(Base):
    __tablename__ = "persons"
    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, unique=True, index=True, nullable=False)
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

class JitlerCache(Base):
    __tablename__ = "jitler_cache"
    id = Column(Integer, primary_key=True)
    query = Column(String, index=True, unique=True)
    type = Column(String)
    response = Column(Text)
    created = Column(Integer)

Base.metadata.create_all(bind=engine)

# ========== СХЕМЫ ==========
class PersonCreate(BaseModel):
    phone: str
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

class PersonResponse(PersonCreate):
    id: int
    class Config:
        from_attributes = True

def get_db():
    db = SessionLocal()
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
    return re.sub(r"\D", "", str(s))

# ========== JITLER ==========
async def jitler_request(type_: str, query: str, page: int = 1) -> dict:
    """Типы: number, sherlock, funstat, vks."""
    if not JITLER_KEYS:
        return {"error": "no jitler keys"}

    async with httpx.AsyncClient(timeout=60) as client:
        for key in JITLER_KEYS:
            try:
                r = await client.post(
                    f"{JITLER_URL}/search",
                    json={"type": type_, "query": query, "page": page},
                    headers={
                        "Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                    },
                )
                if r.status_code in (429, 403):
                    continue  # лимит/блок — берём следующий ключ
                if r.status_code == 200:
                    data = r.json()
                    # А) создана задача
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
                            if r2.status_code not in (501,):
                                break
                    return data
                if r.status_code == 501:
                    continue
            except Exception:
                continue
    return {"error": "all jitler keys failed"}

async def jitler_cached(db: Session, type_: str, query: str, page: int = 1) -> dict:
    cache_key = f"{type_}:{query.lower()}:{page}"
    cached = db.query(JitlerCache).filter(JitlerCache.query == cache_key).first()
    if cached and (time.time() - cached.created) < CACHE_TTL:
        try:
            return json.loads(cached.response)
        except Exception:
            pass

    result = await jitler_request(type_, query, page)

    if "error" not in result:
        if cached:
            cached.response = json.dumps(result, ensure_ascii=False)
            cached.created = int(time.time())
        else:
            db.add(JitlerCache(
                query=cache_key,
                type=type_,
                response=json.dumps(result, ensure_ascii=False),
                created=int(time.time()),
            ))
        db.commit()
    return result

# ========== ПРИЛОЖЕНИЕ ==========
app = FastAPI(title="My Base API")

@app.get("/")
def root():
    return {"status": "ok"}

# -------- 1. ПОИСК ПО НОМЕРУ (база + Jitler number) --------
@app.get("/phone")
async def search_phone(
    q: str,
    page: int = 1,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    norm = normalize_phone(q)
    local = db.query(Person).filter(
        func.regexp_replace(Person.phone, r"\D", "", "g").like(f"%{norm}%")
    ).limit(20).all()

    jitler = await jitler_cached(db, "number", norm, page)

    return {
        "query": q,
        "type": "phone",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
        "external": jitler,
    }

# -------- 2. ПОИСК ПО TELEGRAM ID / @USERNAME --------
@app.get("/telegram")
async def search_telegram(
    q: str,
    page: int = 1,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    local = db.query(Person).filter(
        or_(
            func.lower(Person.telegram).like(f"%{q.lower()}%"),
            func.lower(Person.extra).like(f"%{q.lower()}%"),
        )
    ).limit(20).all()

    clean = q.lstrip("@").strip()

    # Jitler: sherlock + funstat (оба)
    sherlock = await jitler_cached(db, "sherlock", clean, page)
    funstat = await jitler_cached(db, "funstat", clean, page)

    return {
        "query": q,
        "type": "telegram",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
        "external": {
            "sherlock": sherlock,
            "funstat": funstat,
        },
    }

# -------- 3. ПОИСК ПО НИКУ --------
NICKNAME_SITES = {
    "Telegram": "https://t.me/{u}",
    "VK": "https://vk.com/{u}",
    "OK": "https://ok.ru/{u}",
    "GitHub": "https://github.com/{u}",
    "Yandex": "https://yandex.ru/search/?text={u}",
    "Doxbin": "https://doxbin.org/user/{u}",
    "TikTok": "https://www.tiktok.com/@{u}",
    "Roblox": "https://www.roblox.com/search/users?keyword={u}",
    "Twitch": "https://m.twitch.tv/{u}",
    "YouTube": "https://www.youtube.com/{u}",
    "Facebook": "https://www.facebook.com/{u}",
    "Steam": "https://steamcommunity.com/id/{u}",
    "Я.Музыка": "https://music.yandex.ru/users/{u}",
    "Playerok": "https://playerok.com/profile/{u}",
    "Xbox": "https://www.xbox.com/profile/{u}",
    "Trashbox": "https://trashbox.ru/users/{u}",
    "Spotify": "https://open.spotify.com/artist/{u}",
    "Reddit": "https://reddit.com/user/{u}",
    "Trakt": "https://www.trakt.tv/user/{u}",
    "Baddo": "https://baddo.com/en/{u}",
    "OkCupid": "https://www.okcupid.com/profile/{u}",
    "Кинопоиск": "https://www.kinopoisk.ru/mykp/user/{u}",
    "4chan": "https://www.4chan.org/{u}",
    "Minecraft": "https://www.minecraft.net/ru-ru/user/{u}",
    "CodeChef": "https://www.codechef.com/users/{u}",
    "Linux.org": "https://www.linux.org.ru/{u}/people",
    "Guns.ru": "https://forum.guns.ru/forummisc/blog/{u}",
    "Apple Discussions": "https://discussions.apple.com/profile/{u}",
    "Myspace": "https://myspace.com/{u}",
    "Osu": "https://osu.ppy.sh/users/{u}",
    "Codewars": "https://www.codewars.com/users/{u}",
    "Enjin": "https://www.enjin.com/profile/{u}",
    "DDNet": "https://ddnet.org/user/{u}",
}

@app.get("/nickname")
def search_nickname(
    q: str,
    api_key: str = Depends(check_api_key),
):
    links = {name: url.format(u=q) for name, url in NICKNAME_SITES.items()}
    return {
        "query": q,
        "type": "nickname",
        "gmail": f"{q}@gmail.com",
        "links": links,
    }

# -------- 4. ПОИСК ПО ДОКУМЕНТАМ --------
@app.get("/documents")
def search_documents(
    q: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    local = db.query(Person).filter(
        or_(
            func.lower(Person.passport).like(f"%{q.lower()}%"),
            func.lower(Person.snils).like(f"%{q.lower()}%"),
            func.lower(Person.inn).like(f"%{q.lower()}%"),
        )
    ).limit(20).all()
    return {
        "query": q,
        "type": "documents",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
    }

# -------- 5. ПОИСК ПО ФИО --------
@app.get("/fio")
def search_fio(
    q: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    local = db.query(Person).filter(
        func.lower(Person.fio).like(f"%{q.lower()}%")
    ).limit(20).all()
    return {
        "query": q,
        "type": "fio",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
    }

# -------- 6. ПОИСК ПО АДРЕСУ --------
@app.get("/address")
def search_address(
    q: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    local = db.query(Person).filter(
        func.lower(Person.address).like(f"%{q.lower()}%")
    ).limit(20).all()
    return {
        "query": q,
        "type": "address",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
    }

# -------- 7. ДОПОЛНИТЕЛЬНЫЙ ПОИСК (extra) --------
@app.get("/extra")
def search_extra(
    q: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    local = db.query(Person).filter(
        func.lower(Person.extra).like(f"%{q.lower()}%")
    ).limit(20).all()
    return {
        "query": q,
        "type": "extra",
        "local": [PersonResponse.model_validate(p).model_dump() for p in local],
    }

# -------- 8. VKS (Jitler vks) --------
@app.get("/vks")
async def search_vks(
    q: str,
    page: int = 1,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db),
):
    jitler = await jitler_cached(db, "vks", q, page)
    return {"query": q, "type": "vks", "external": jitler}

# -------- ВСПОМОГАТЕЛЬНЫЕ --------
@app.get("/persons", response_model=List[PersonResponse])
def get_all(skip: int = 0, limit: int = 100, api_key: str = Depends(check_api_key), db: Session = Depends(get_db)):
    return db.query(Person).offset(skip).limit(limit).all()

@app.get("/persons/{phone}", response_model=PersonResponse)
def get_by_phone(phone: str, api_key: str = Depends(check_api_key), db: Session = Depends(get_db)):
    norm = normalize_phone(phone)
    person = db.query(Person).filter(func.regexp_replace(Person.phone, r"\D", "", "g") == norm).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    return person

@app.post("/persons", response_model=PersonResponse)
def create_person(person: PersonCreate, api_key: str = Depends(check_api_key), db: Session = Depends(get_db)):
    person.phone = normalize_phone(person.phone)
    existing = db.query(Person).filter(Person.phone == person.phone).first()
    if existing:
        for k, v in person.model_dump().items():
            if v is not None:
                setattr(existing, k, v)
        db.commit()
        db.refresh(existing)
        return existing
    db_person = Person(**person.model_dump())
    db.add(db_person)
    db.commit()
    db.refresh(db_person)
    return db_person

@app.delete("/persons/{phone}")
def delete_person(phone: str, api_key: str = Depends(check_api_key), db: Session = Depends(get_db)):
    norm = normalize_phone(phone)
    person = db.query(Person).filter(func.regexp_replace(Person.phone, r"\D", "", "g") == norm).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    db.delete(person)
    db.commit()
    return {"status": "deleted", "phone": norm}
