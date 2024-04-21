#!/usr/bin/env python
# coding: utf-8

# # LA Crime: Database models

# #### Load Python tools and Jupyter config

# In[73]:


import json
import requests
import pandas as pd
import jupyter_black
import altair as alt
import geopandas as gpd
import sqlalchemy
from sqlalchemy import (
    create_engine,
    text,
    Column,
    Integer,
    String,
    Float,
    ForeignKey,
    DateTime,
)
from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    ForeignKey,
    DateTime,
    Float,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker


# In[2]:


jupyter_black.load()
pd.options.display.max_columns = 100
pd.options.display.max_rows = 1000
pd.options.display.max_colwidth = None


# In[3]:


today = pd.Timestamp("today").strftime("%Y%m%d")
today_epoch = int(pd.Timestamp.today().timestamp())


# ---

# ## Read

# In[6]:


metadata = pd.read_csv("../data/raw/crime_data_metadata.csv")


# In[7]:


types_dict = dict(zip(metadata["name"], metadata["dataTypeName"]))


# In[8]:


df_types = {
    "DR_NO": str,
    "TIME OCC": str,
    "AREA ": str,
    "AREA NAME": str,
    "Rpt Dist No": str,
    "Part 1-2": str,
    "Crm Cd": str,
    "Crm Cd Desc": str,
    "Mocodes": str,
    "Vict Age": str,
    "Vict Sex": str,
    "Vict Descent": str,
    "Premis Cd": str,
    "Premis Desc": str,
    "Weapon Used Cd": str,
    "Weapon Desc": str,
    "Status": str,
    "Status Desc": str,
    "Crm Cd 1": str,
    "Crm Cd 2": str,
    "Crm Cd 3": str,
    "Crm Cd 4": str,
    "LOCATION": str,
    "Cross Street": str,
    "LAT": "float",
    "LON": "float",
    "AREA": str,
    "AREA ": str,
}


# #### 2010-2019 dataset

# In[9]:


df_2010_2019 = pd.read_csv(
    "../data/raw/crime_data_2010_2019.csv",
    dtype=df_types,
)
df_2010_2019.columns = df_2010_2019.columns.str.strip()


# #### 2020-present dataset

# In[10]:


df_2020_present = pd.read_csv(
    "../data/raw/crime_data_2020_present.csv",
    dtype=df_types,
)
df_2020_present.columns = df_2020_present.columns.str.strip()


# #### Concatenate the two dataframes

# In[ ]:


df = pd.concat([df_2010_2019, df_2020_present]).reset_index(drop=True)


# ---

# ## Process

# #### Clean column names

# In[12]:


rename_dict = dict(zip(metadata["name"], metadata["fieldName"]))

df = df.rename(columns=rename_dict)


# #### Convert date columns to datetime

# In[13]:


df["date_rptd"] = pd.to_datetime(
    df["date_rptd"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce"
)
df["date_occ"] = pd.to_datetime(
    df["date_occ"], format="%m/%d/%Y %I:%M:%S %p", errors="coerce"
)


# #### Convert the military time column

# In[14]:


df["time_occ"] = pd.to_datetime(df["time_occ"], format="%H%M").dt.time


# #### Combine "date_occ" and "time_occ" into a new "datetime_occurred" column

# In[15]:


df["datetime_occurred"] = pd.to_datetime(
    df["date_occ"].dt.date.astype(str) + " " + df["time_occ"].astype(str)
)


# #### Strip any stray characters from str columns

# In[16]:


def strip_whitespace(df):
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
        elif any(df[col].apply(lambda x: isinstance(x, str))):
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


# In[ ]:


df = strip_whitespace(df)


# #### Ensure there are no duplicates

# In[65]:


len(df)


# In[ ]:


df = df.drop_duplicates(subset="dr_no", keep="last")


# In[67]:


len(df)


# ---

# ## Normalization
# > Extract unique combinations of codes and descriptions for separate tables: crimes, premiseses, weapons, areas, etc. 

# #### Crime codes

# In[18]:


unique_crime_codes = (
    df[["crm_cd", "crm_cd_desc"]].drop_duplicates().reset_index(drop=True)
)


# #### Area codes

# In[19]:


unique_area_codes = df[["area", "area_name"]].drop_duplicates().reset_index(drop=True)


# #### Premises codes

# In[20]:


unique_premis_codes = (
    df[["premis_cd", "premis_desc"]].drop_duplicates().reset_index(drop=True)
)
unique_premis_codes = unique_premis_codes.query('premis_desc != "nan"')


# #### Weapons codes

# In[21]:


unique_weapon_codes = (
    df[["weapon_used_cd", "weapon_desc"]].drop_duplicates().reset_index(drop=True)
)
unique_weapon_codes = unique_weapon_codes.query('weapon_desc != "nan"')


# #### Status codes

# In[22]:


unique_status_codes = (
    df[["status", "status_desc"]].drop_duplicates().reset_index(drop=True)
)


# In[23]:


unique_status_codes["status_desc"] = (
    unique_status_codes["status"]
    .map(
        {
            "AA": "Adult Arrest",
            "IC": "Invest Cont",
            "AO": "Adult Other",
            "JA": "Juvenile Arrest",
            "JO": "Juvenile Other",
            "nan": "Unknown/Other",
            "CC": "Unknown/Other",
            "TH": "Unknown/Other",
            "13": "Unknown/Other",
            "19": "Unknown/Other",
        }
    )
    .fillna("Unknown/Other")
)


# #### Clean up sex and create unique table

# In[24]:


def clean_vict_sex(value):
    if value in ["M", "F"]:
        return value
    else:
        return "U"  # Unknown/Other


# In[25]:


df["vict_sex"] = df["vict_sex"].apply(clean_vict_sex)


# In[26]:


df["vict_sex_description"] = (
    df["vict_sex"]
    .map({"F": "Female", "M": "Male", "U": "Unknown/Other"})
    .fillna("Unknown/Other")
)


# In[27]:


unique_sex_codes = (
    df[["vict_sex", "vict_sex_description"]].drop_duplicates().reset_index(drop=True)
)


# ---

# ## Models

# #### Define for lookups and main incidents

# In[60]:


Base = declarative_base()


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


# ---

# ## Insert

# #### Create tables in the database

# In[42]:


engine = create_engine("postgresql+psycopg2://mstiles:password@localhost/crime_data")
Base.metadata.create_all(engine)


# #### Insert data into status table

# In[50]:


unique_status_codes.columns


# In[ ]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_status_codes.iterrows():
    status = Status(status=row["status"], status_desc=row["status_desc"])
    session.add(status)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into weapons table

# In[49]:


unique_weapon_codes.columns


# In[48]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_weapon_codes.iterrows():
    weapon = Weapon(
        weapon_used_cd=row["weapon_used_cd"], weapon_desc=row["weapon_desc"]
    )
    session.add(weapon)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into sex table

# In[51]:


unique_sex_codes.columns


# In[52]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_sex_codes.iterrows():
    victim_sex = VictimSex(
        vict_sex=row["vict_sex"], vict_sex_description=row["vict_sex_description"]
    )
    session.add(victim_sex)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into premises table

# In[53]:


unique_premis_codes.columns


# In[54]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_premis_codes.iterrows():
    premise = Premise(premis_cd=row["premis_cd"], premis_desc=row["premis_desc"])
    session.add(premise)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into areas table

# In[56]:


unique_area_codes.columns


# In[ ]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_area_codes.iterrows():
    area = Area(area=row["area"], area_name=row["area_name"])
    session.add(area)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into crimes table

# In[58]:


unique_crime_codes.columns


# In[ ]:


Session = sessionmaker(bind=engine)
session = Session()

for index, row in unique_crime_codes.iterrows():
    crime_code = CrimeCode(crm_cd=row["crm_cd"], crm_cd_desc=row["crm_cd_desc"])
    session.add(crime_code)

try:
    session.commit()
except Exception as e:
    session.rollback()
    raise
finally:
    session.close()


# #### Insert data into incidents table

# In[68]:


incidents_list = df.to_dict(orient="records")

Session = sessionmaker(bind=engine)
session = Session()

try:
    session.bulk_insert_mappings(Incident, incidents_list)
    session.commit()
except Exception as e:
    print(f"An error occurred: {e}")
    session.rollback()
finally:
    session.close()


# In[69]:


# incident_count = session.query(Incident).count()
# print(f"Number of incidents currently in the database: {incident_count}")


# In[ ]:





# In[74]:





# In[ ]:





# In[ ]:




