import os, re
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String, func, or_
from sqlalchemy.orm import declarative_base, sessionmaker, Session

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

Base.metadata.create_all(bind=engine)

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
    """Оставить только цифры и ведущий +."""
    if not s:
        return ""
    s = str(s).strip()
    plus = s.startswith("+")
    digits = re.sub(r"\D", "", s)
    return ("+" if plus else "") + digits

app = FastAPI(title="My Base API")

@app.get("/")
def root():
    return {"status": "ok", "message": "API работает"}

@app.get("/search", response_model=List[PersonResponse])
def search(
    q: str,
    limit: int = 50,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    # 1) обычный поиск — регистронезависимый
    q_lower = f"%{q.lower()}%"
    q_digits = f"%{re.sub(r'\\D', '', q)}%" if re.search(r"\\d", q) else None

    conditions = [
        func.lower(Person.phone).like(q_lower),
        func.lower(Person.fio).like(q_lower),
        func.lower(Person.dob).like(q_lower),
        func.lower(Person.operator).like(q_lower),
        func.lower(Person.region).like(q_lower),
        func.lower(Person.country).like(q_lower),
        func.lower(Person.phonebooks).like(q_lower),
        func.lower(Person.passport).like(q_lower),
        func.lower(Person.snils).like(q_lower),
        func.lower(Person.inn).like(q_lower),
        func.lower(Person.address).like(q_lower),
        func.lower(Person.work).like(q_lower),
        func.lower(Person.banks).like(q_lower),
        func.lower(Person.email).like(q_lower),
        func.lower(Person.telegram).like(q_lower),
        func.lower(Person.vk).like(q_lower),
        func.lower(Person.ok).like(q_lower),
        func.lower(Person.instagram).like(q_lower),
        func.lower(Person.tiktok).like(q_lower),
        func.lower(Person.max_link).like(q_lower),
        func.lower(Person.whatsapp).like(q_lower),
        func.lower(Person.car).like(q_lower),
        func.lower(Person.extra).like(q_lower),
    ]

    # 2) если запрос — номер, ищем по цифрам без + и разделителей
    if q_digits:
        conditions.append(func.regexp_replace(Person.phone, r"\\D", "", "g").like(q_digits))

    return db.query(Person).filter(or_(*conditions)).limit(limit).all()

@app.get("/persons", response_model=List[PersonResponse])
def get_all(
    skip: int = 0,
    limit: int = 100,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    return db.query(Person).offset(skip).limit(limit).all()

@app.get("/persons/{phone}", response_model=PersonResponse)
def get_by_phone(
    phone: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    norm = normalize_phone(phone)
    person = db.query(Person).filter(Person.phone == norm).first()
    if not person:
        # попробуем без +
        person = db.query(Person).filter(Person.phone == norm.lstrip("+")).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    return person

@app.post("/persons", response_model=PersonResponse)
def create_person(
    person: PersonCreate,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
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
def delete_person(
    phone: str,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    norm = normalize_phone(phone)
    person = db.query(Person).filter(Person.phone == norm).first()
    if not person:
        person = db.query(Person).filter(Person.phone == norm.lstrip("+")).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    db.delete(person)
    db.commit()
    return {"status": "deleted", "phone": norm}
