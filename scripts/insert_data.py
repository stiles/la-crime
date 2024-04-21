#!/usr/bin/env python
# coding: utf-8

"""
Insert main incidents table and lookups into the db
"""

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from database_models import Status, Weapon, VictimSex, Premise, Area, CrimeCode, Incident

# Connect to the database
DATABASE_URI = "postgresql+psycopg2://mstiles:password@localhost/crime_data"
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)

# Reading processed data
unique_sex_codes = pd.read_parquet('../data/processed/unique_sex_codes.parquet')
unique_status_codes = pd.read_parquet('../data/processed/unique_status_codes.parquet')
unique_weapon_codes = pd.read_parquet('../data/processed/unique_weapon_codes.parquet')
unique_premis_codes = pd.read_parquet('../data/processed/unique_premis_codes.parquet')
unique_crime_codes = pd.read_parquet('../data/processed/unique_crime_codes.parquet')
unique_area_codes = pd.read_parquet('../data/processed/unique_area_codes.parquet')
incidents_df = pd.read_parquet('../data/processed/incidents_df.parquet')

def insert_lookup_data(df, model):
    """
    Inserts data into a lookup table.
    """
    session = Session()
    try:
        for index, row in df.iterrows():
            record = model(**row.to_dict())
            session.add(record)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"An error occurred: {e}")
    finally:
        session.close()

def insert_incidents_data(df):
    """
    Inserts data into the incidents table.
    """
    session = Session()
    incidents_list = df.to_dict(orient="records")
    try:
        session.bulk_insert_mappings(Incident, incidents_list)
        session.commit()
    except Exception as e:
        print(f"An error occurred: {e}")
        session.rollback()
    finally:
        session.close()

# Uncomment the lines below to insert data into the database
insert_lookup_data(unique_status_codes, Status)
insert_lookup_data(unique_weapon_codes, Weapon)
insert_lookup_data(unique_sex_codes, VictimSex)
insert_lookup_data(unique_premis_codes, Premise)
insert_lookup_data(unique_area_codes, Area)
insert_lookup_data(unique_crime_codes, CrimeCode)
insert_incidents_data(incidents_df)