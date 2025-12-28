from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base, Session
import aiohttp
import asyncio
import csv
from datetime import datetime, timedelta


Base = declarative_base()


class City(Base):

    __tablename__ = "cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)
    temperature = Column(Float, nullable=True)
    updated_at = Column(DateTime, nullable=True)


class DefaultCity(Base):

    __tablename__ = "default_cities"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    latitude = Column(Float)
    longitude = Column(Float)


DATABASE_URL = "sqlite:///./cities.db"
engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base.metadata.create_all(bind=engine)


app = FastAPI(title="Weather App")
templates = Jinja2Templates(directory="templates")


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


async def fetch_weather(session: aiohttp.ClientSession, latitude: float, longitude: float):
    url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current_weather=true"
    try:
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data.get("current_weather", {}).get("temperature")
    except Exception as e:
        print(f"Error fetching weather for ({latitude}, {longitude}): {e}")
    return None


async def fetch_all_weather(cities: list) -> list:
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_weather(session, city.latitude, city.longitude)
            for city in cities
        ]
        return await asyncio.gather(*tasks)



@app.on_event("startup")
def startup_event():

    db = SessionLocal()
    try:

        if not db.query(DefaultCity).first():
            try:
                with open("cities.csv", "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        db.add(DefaultCity(
                            name=row["city"],
                            latitude=float(row["latitude"]),
                            longitude=float(row["longitude"])
                        ))
                db.commit()
                print("Default cities loaded from CSV")
            except FileNotFoundError:
                print("Warning: cities.csv not found")

        if not db.query(City).first():
            default_cities = db.query(DefaultCity).all()
            for dc in default_cities:
                db.add(City(
                    name=dc.name,
                    latitude=dc.latitude,
                    longitude=dc.longitude,
                    temperature=None,
                    updated_at=None
                ))
            db.commit()
            print("Cities table initialized from defaults")
    finally:
        db.close()



@app.get("/")
async def read_root(request: Request, db: Session = Depends(get_db)):

    cities = db.query(City).order_by(
        City.temperature.desc().nullslast()
    ).all()
    return templates.TemplateResponse(
        "index.html",
        {"request": request, "cities": cities}
    )


@app.post("/cities/remove/{city_id}")
async def remove_city(city_id: int, db: Session = Depends(get_db)):

    city = db.query(City).filter(City.id == city_id).first()
    if city:
        db.delete(city)
        db.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/cities/reset")
async def reset_cities(db: Session = Depends(get_db)):

    db.query(City).delete()

    default_cities = db.query(DefaultCity).all()
    for dc in default_cities:
        db.add(City(
            name=dc.name,
            latitude=dc.latitude,
            longitude=dc.longitude,
            temperature=None,
            updated_at=None
        ))
    db.commit()
    return RedirectResponse("/", status_code=303)


@app.post("/cities/update")
async def update_weather(db: Session = Depends(get_db)):

    cities = db.query(City).all()
    now = datetime.utcnow()


    cities_to_update = []
    for city in cities:
        if city.updated_at is None or (now - city.updated_at) >= timedelta(minutes=15):
            cities_to_update.append(city)

    if cities_to_update:

        temperatures = await fetch_all_weather(cities_to_update)


        for city, temp in zip(cities_to_update, temperatures):
            if temp is not None:
                city.temperature = temp
                city.updated_at = now

        db.commit()

    return RedirectResponse("/", status_code=303)



@app.post("/cities/add")
async def add_city(
    name: str = Form(...),
    latitude: float = Form(...),
    longitude: float = Form(...),
    db: Session = Depends(get_db)
):

    existing = db.query(City).filter(City.name == name).first()
    if not existing:
        city = City(
            name=name,
            latitude=latitude,
            longitude=longitude,
            temperature=None,
            updated_at=None
        )
        db.add(city)
        db.commit()
    return RedirectResponse("/", status_code=303)

