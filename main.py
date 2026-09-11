import os
from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel
from typing import Optional, List
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base, sessionmaker, Session

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./mybase.db")
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(bind=engine)
Base = declarative_base()

class Person(Base):
    __tablename__ = "persons"
    id = Column(Integer, primary_key=True, index=True)
    phone = Column(String, unique=True, index=True, nullable=False)
    fio = Column(String)
    operator = Column(String)
    region = Column(String)
    phonebooks = Column(String)
    telegram = Column(String)
    max_link = Column(String)
    whatsapp = Column(String)
    extra = Column(String)

Base.metadata.create_all(bind=engine)

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

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

app = FastAPI(title="My Base API")

@app.get("/")
def root():
    return {"status": "ok"}

@app.get("/persons", response_model=List[PersonResponse])
def get_all(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return db.query(Person).offset(skip).limit(limit).all()

@app.get("/persons/{phone}", response_model=PersonResponse)
def get_by_phone(phone: str, db: Session = Depends(get_db)):
    person = db.query(Person).filter(Person.phone == phone).first()
    if not person:
        raise HTTPException(status_code=404, detail="Не найдено")
    return person

@app.post("/persons", response_model=PersonResponse)
def create_person(person: PersonCreate, db: Session = Depends(get_db)):
    existing = db.query(Person).filter(Person.phone == person.phone).first()
    if existing:
        raise HTTPException(status_code=400, detail="Номер уже есть")
    db_person = Person(**person.model_dump())
    db.add(db_person)
    db.commit()
    db.refresh(db_person)
    return db_person