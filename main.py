import os
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, or_
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ========== НАСТРОЙКИ ==========
API_KEY = os.getenv("API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL") or "sqlite:///./mybase.db"

if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ========== МОДЕЛЬ (все поля) ==========
class Person(Base):
    __tablename__ = "persons"
    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, unique=True, index=True, nullable=False)
    fio = Column(String, index=True, nullable=True)
    dob = Column(String, nullable=True)            # дата рождения
    operator = Column(String, nullable=True)
    region = Column(String, nullable=True)
    country = Column(String, nullable=True)
    phonebooks = Column(String, nullable=True)
    passport = Column(String, nullable=True)       # паспорт
    snils = Column(String, nullable=True)          # СНИЛС
    inn = Column(String, nullable=True)            # ИНН
    address = Column(String, nullable=True)        # адрес
    work = Column(String, nullable=True)           # работа
    banks = Column(String, nullable=True)          # банки
    email = Column(String, nullable=True)          # почта
    telegram = Column(String, nullable=True)       # все telegram через запятую
    vk = Column(String, nullable=True)
    ok = Column(String, nullable=True)
    instagram = Column(String, nullable=True)
    tiktok = Column(String, nullable=True)
    max_link = Column(String, nullable=True)
    whatsapp = Column(String, nullable=True)
    car = Column(String, nullable=True)
    extra = Column(String, nullable=True)

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

# ========== ЗАВИСИМОСТИ ==========
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

# ========== ПРИЛОЖЕНИЕ ==========
app = FastAPI(title="My Base API")

@app.get("/")
def root():
    return {"status": "ok", "message": "API работает"}

# Универсальный поиск по любому полю
@app.get("/search", response_model=List[PersonResponse])
def search(
    q: str,
    limit: int = 50,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    like = f"%{q}%"
    return db.query(Person).filter(
        or_(
            Person.phone.like(like),
            Person.fio.like(like),
            Person.dob.like(like),
            Person.operator.like(like),
            Person.region.like(like),
            Person.phonebooks.like(like),
            Person.passport.like(like),
            Person.snils.like(like),
            Person.inn.like(like),
            Person.address.like(like),
            Person.work.like(like),
            Person.banks.like(like),
            Person.email.like(like),
            Person.telegram.like(like),
            Person.vk.like(like),
            Person.ok.like(like),
            Person.instagram.like(like),
            Person.tiktok.like(like),
            Person.max_link.like(like),
            Person.whatsapp.like(like),
            Person.car.like(like),
            Person.extra.like(like),
        )
    ).limit(limit).all()

# Получить всех
@app.get("/persons", response_model=List[PersonResponse])
def get_all(
    skip: int = 0,
    limit: int = 100,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    return db.query(Person).offset(skip).limit(limit).all()

# Найти по номеру (точное совпадение)
@app.get("/persons/{phone}", response_model=PersonResponse)
def get_by_phone(
    phone: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    person = db.query(Person).filter(Person.phone == phone).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    return person

# Добавить нового
@app.post("/persons", response_model=PersonResponse)
def create_person(
    person: PersonCreate,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    existing = db.query(Person).filter(Person.phone == person.phone).first()
    if existing:
        # обновляем существующую запись
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
