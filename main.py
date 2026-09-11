import os
from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session

# ========== НАСТРОЙКИ ==========
API_KEY = os.getenv("API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./mybase.db")

if DATABASE_URL and DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

# ========== МОДЕЛЬ ==========
class Person(Base):
    __tablename__ = "persons"
    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, unique=True, index=True, nullable=False)
    fio = Column(String, nullable=True)
    operator = Column(String, nullable=True)
    region = Column(String, nullable=True)
    phonebooks = Column(String, nullable=True)
    telegram = Column(String, nullable=True)
    max_link = Column(String, nullable=True)
    whatsapp = Column(String, nullable=True)
    extra = Column(String, nullable=True)

Base.metadata.create_all(bind=engine)

# ========== СХЕМЫ ==========
class PersonCreate(BaseModel):
    phone: str
    fio: Optional[str] = None
    operator: Optional[str] = None
    region: Optional[str] = None
    phonebooks: Optional[str] = None
    telegram: Optional[str] = None
    max_link: Optional[str] = None
    whatsapp: Optional[str] = None
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
    person = db.query(Person).filter(Person.phone == phone).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    return person

@app.post("/persons", response_model=PersonResponse)
def create_person(
    person: PersonCreate,
    api_key: str = Depends(check_api_key),
    db: Session = Depends(get_db)
):
    existing = db.query(Person).filter(Person.phone == person.phone).first()
    if existing:
        raise HTTPException(status_code=400, detail="Номер уже есть")
    db_person = Person(**person.model_dump())
    db.add(db_person)
    db.commit()
    db.refresh(db_person)
    return db_person
