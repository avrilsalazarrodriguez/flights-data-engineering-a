from sqlalchemy import (
    String,
    Integer,
    Float,
    ForeignKey
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Airline(Base):
    __tablename__ = "airlines"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airline: Mapped[str] = mapped_column(String(255), nullable=False)


class Airport(Base):
    __tablename__ = "airports"

    iata_code: Mapped[str] = mapped_column(String(10), primary_key=True)
    airport: Mapped[str] = mapped_column(String(255), nullable=False)
    city: Mapped[str] = mapped_column(String(255), nullable=True)
    state: Mapped[str] = mapped_column(String(50), nullable=True)


class Flight(Base):
    __tablename__ = "flights"

    flight_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[int] = mapped_column(Integer, nullable=True)
    month: Mapped[int] = mapped_column(Integer, nullable=True)
    day: Mapped[int] = mapped_column(Integer, nullable=True)

    airline: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airlines.iata_code"),
        nullable=True
    )

    origin_airport: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=True
    )

    destination_airport: Mapped[str] = mapped_column(
        String(10),
        ForeignKey("airports.iata_code"),
        nullable=True
    )

    departure_delay: Mapped[float] = mapped_column(Float, nullable=True)
    arrival_delay: Mapped[float] = mapped_column(Float, nullable=True)
    cancelled: Mapped[int] = mapped_column(Integer, nullable=True)
    cancellation_reason: Mapped[str] = mapped_column(String(10), nullable=True)
    distance: Mapped[float] = mapped_column(Float, nullable=True)
    air_system_delay: Mapped[float] = mapped_column(Float, nullable=True)
    airline_delay: Mapped[float] = mapped_column(Float, nullable=True)
    weather_delay: Mapped[float] = mapped_column(Float, nullable=True)
    late_aircraft_delay: Mapped[float] = mapped_column(Float, nullable=True)
    security_delay: Mapped[float] = mapped_column(Float, nullable=True)