#!/usr/bin/env python
# coding: utf-8

"""
LA Crime: Define the database models
"""

import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, ForeignKey
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Define your database connection string
DATABASE_URI = "postgresql+psycopg2://mstiles:password@localhost/crime_data"

# Create the engine that will connect to the database
engine = create_engine(DATABASE_URI, echo=True)  # Set echo=False in production
Session = sessionmaker(bind=engine)

# Declare a base for your models to inherit from
Base = declarative_base()

"""
Models for lookups and main incidents tables
"""

class Status(Base):
    __tablename__ = "status_codes"
    id = Column(Integer, primary_key=True)
    status = Column(String, unique=True)
    status_desc = Column(String)

class Weapon(Base):
    __tablename__ = "weapon_codes"
    id = Column(Integer, primary_key=True)
    weapon_used_cd = Column(String, unique=True)
    weapon_desc = Column(String)

class Premise(Base):
    __tablename__ = "premise_codes"
    id = Column(Integer, primary_key=True)
    premis_cd = Column(String, unique=True)
    premis_desc = Column(String)

class Area(Base):
    __tablename__ = "area_codes"
    id = Column(Integer, primary_key=True)
    area = Column(String, unique=True)
    area_name = Column(String)

class CrimeCode(Base):
    __tablename__ = "crime_codes"
    id = Column(Integer, primary_key=True)
    crm_cd = Column(String, unique=True)
    crm_cd_desc = Column(String)

class VictimSex(Base):
    __tablename__ = "sex_codes"
    id = Column(Integer, primary_key=True)
    vict_sex = Column(String, unique=True)
    vict_sex_description = Column(String)

class Incident(Base):
    __tablename__ = "incidents"
    dr_no = Column(String, primary_key=True)
    date_rptd = Column(DateTime)
    date_occ = Column(DateTime)
    time_occ = Column(String)
    area_id = Column(Integer, ForeignKey("area_codes.id"))
    rpt_dist_no = Column(String)
    part_1_2 = Column(String)
    crm_cd_id = Column(Integer, ForeignKey("crime_codes.id"))
    mocodes = Column(String)
    vict_age = Column(String)
    vict_sex_id = Column(Integer, ForeignKey("sex_codes.id"))
    premis_cd_id = Column(Integer, ForeignKey("premise_codes.id"))
    weapon_used_cd_id = Column(Integer, ForeignKey("weapon_codes.id"))
    status_id = Column(Integer, ForeignKey("status_codes.id"))
    crm_cd_1 = Column(String)
    crm_cd_2 = Column(String)
    crm_cd_3 = Column(String)
    crm_cd_4 = Column(String)
    location = Column(String)
    cross_street = Column(String)
    lat = Column(Float)
    lon = Column(Float)
    datetime_occurred = Column(DateTime)

    # Relationships
    area = relationship("Area")
    crime_code = relationship("CrimeCode")
    victim_sex = relationship("VictimSex")
    premise = relationship("Premise")
    weapon = relationship("Weapon")
    status = relationship("Status")

# Create tables in the database
Base.metadata.create_all(engine)