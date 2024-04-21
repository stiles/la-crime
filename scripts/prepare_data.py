#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from database_models import engine, Session, Area, CrimeCode, Premise, Weapon, Status, VictimSex


# Utility functions
def strip_whitespace(df):
    """
    Strip away any white space from string columns in a DataFrame.
    """
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].astype(str).str.strip()
    return df

def load_data(file_path, dtype_dict):
    """
    Load data from a CSV file with specific data types.
    """
    df = pd.read_csv(file_path, dtype=dtype_dict)
    df.columns = df.columns.str.strip()
    return df

def clean_and_combine(df1, df2, rename_dict):
    """
    Clean and combine two DataFrames, rename columns, convert date and time columns to datetime,
    and ensure there are no duplicate DR_NO entries.
    """
    df_combined = pd.concat([df1, df2]).reset_index(drop=True)
    df_combined = df_combined.rename(columns=rename_dict)
    df_combined = strip_whitespace(df_combined)

    # Convert date and time columns to datetime
    df_combined["date_rptd"] = pd.to_datetime(df_combined["date_rptd"], format='%m/%d/%Y', errors='coerce')
    df_combined["date_occ"] = pd.to_datetime(df_combined["date_occ"], format='%m/%d/%Y', errors='coerce')
    df_combined["time_occ"] = pd.to_datetime(df_combined["time_occ"], format='%H%M', errors='coerce').dt.time

    # Combine "date_occ" and "time_occ" into "datetime_occurred"
    df_combined["datetime_occurred"] = pd.to_datetime(df_combined["date_occ"].dt.strftime('%Y-%m-%d') + ' ' + df_combined["time_occ"].astype(str))

    # Example for converting 'NaT' to 'None' in 'date_rptd', 'date_occ', and 'datetime_occurred'
    df_combined['date_rptd'] = df_combined['date_rptd'].apply(lambda x: None if pd.isnull(x) else x)
    df_combined['date_occ'] = df_combined['date_occ'].apply(lambda x: None if pd.isnull(x) else x)
    df_combined['datetime_occurred'] = df_combined['datetime_occurred'].apply(lambda x: None if pd.isnull(x) else x)

    
    df_combined = df_combined.drop_duplicates(subset="dr_no", keep="last")
    
    return df_combined

# Normalization functions
def normalize_crime_codes(df):
    return df[["crm_cd", "crm_cd_desc"]].drop_duplicates().reset_index(drop=True)

def normalize_area_codes(df):
    return df[["area", "area_name"]].drop_duplicates().reset_index(drop=True)

def normalize_premis_codes(df):
    return df.query('premis_desc != "nan"')[["premis_cd", "premis_desc"]].drop_duplicates().reset_index(drop=True)

def normalize_weapon_codes(df):
    return df.query('weapon_desc != "nan"')[["weapon_used_cd", "weapon_desc"]].drop_duplicates().reset_index(drop=True)

def normalize_status_codes(df):
    df["status_desc"] = df["status"].map({
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
    }).fillna("Unknown/Other")

    return df[["status", "status_desc"]].drop_duplicates().reset_index(drop=True)

def normalize_vict_sex(df):
    # Map 'M' to "Male", 'F' to "Female", and all other values to "Unknown/Other"
    sex_mapping = {
        "M": "Male",
        "F": "Female",
        "X": "Unknown/Other",  # Assuming 'X' and other non-standard values mean unknown or not specified
        "H": "Unknown/Other",
        "-": "Unknown/Other",
        "N": "Unknown/Other"
    }
    # Apply mapping and handle NaN values
    df["vict_sex"] = df["vict_sex"].map(sex_mapping).fillna("Unknown/Other")
    
    # After mapping, create a unique DataFrame for normalized sex codes
    unique_sex_codes = df[["vict_sex"]].drop_duplicates().reset_index(drop=True)
    
    return unique_sex_codes

def save_processed_data(df_dict):
    """
    Save processed DataFrames to parquet files.
    """
    for name, df in df_dict.items():
        df.to_parquet(f'../data/processed/{name}.parquet', index=False)

def main():
    dtype_dict = {
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
    
    rename_dict = {
        'DR_NO': 'dr_no',
        'Date Rptd': 'date_rptd',
        'DATE OCC': 'date_occ',
        'TIME OCC': 'time_occ',
        'AREA ': 'area',
        'AREA NAME': 'area_name',
        'Rpt Dist No': 'rpt_dist_no',
        'Part 1-2': 'part_1_2',
        'Crm Cd': 'crm_cd',
        'Crm Cd Desc': 'crm_cd_desc',
        'Mocodes': 'mocodes',
        'Vict Age': 'vict_age',
        'Vict Sex': 'vict_sex',
        'Vict Descent': 'vict_descent',
        'Premis Cd': 'premis_cd',
        'Premis Desc': 'premis_desc',
        'Weapon Used Cd': 'weapon_used_cd',
        'Weapon Desc': 'weapon_desc',
        'Status': 'status',
        'Status Desc': 'status_desc',
        'Crm Cd 1': 'crm_cd_1',
        'Crm Cd 2': 'crm_cd_2',
        'Crm Cd 3': 'crm_cd_3',
        'Crm Cd 4': 'crm_cd_4',
        'LOCATION': 'location',
        'Cross Street': 'cross_street',
        'LAT': 'lat',
        'LON': 'lon',
        'AREA': 'area'
        }

    df_2010_2019 = load_data("../data/raw/crime_data_2010_2019.csv", dtype_dict)
    df_2020_present = load_data("../data/raw/crime_data_2020_present.csv", dtype_dict)
    
    df_combined = clean_and_combine(df_2010_2019, df_2020_present, rename_dict)

    # Normalize data and save processed dataframes
    unique_dfs = {
        "unique_crime_codes": normalize_crime_codes(df_combined),
        "unique_area_codes": normalize_area_codes(df_combined),
        "unique_premis_codes": normalize_premis_codes(df_combined),
        "unique_weapon_codes": normalize_weapon_codes(df_combined),
        "unique_status_codes": normalize_status_codes(df_combined),
        "unique_sex_codes": normalize_vict_sex(df_combined),
    }
    save_processed_data(unique_dfs)

    session = Session()
    
    def fetch_mapping(session, model, code_column, id_column='id'):
        """Generic function to fetch a mapping dictionary for any lookup table."""
        mapping = {getattr(row, code_column): getattr(row, id_column) for row in session.query(model).all()}
        return mapping
    
    session = Session()

    # Fetch mappings
    area_id_mapping = fetch_mapping(session, Area, 'area')
    crime_id_mapping = fetch_mapping(session, CrimeCode, 'crm_cd')
    premis_id_mapping = fetch_mapping(session, Premise, 'premis_cd')
    weapon_id_mapping = fetch_mapping(session, Weapon, 'weapon_used_cd')
    status_id_mapping = fetch_mapping(session, Status, 'status')
    sex_id_mapping = fetch_mapping(session, VictimSex, 'vict_sex')

    # Apply mappings
    df_combined['area_id'] = df_combined['area'].map(area_id_mapping)
    df_combined['crm_cd_id'] = df_combined['crm_cd'].map(crime_id_mapping)
    df_combined['premis_cd_id'] = df_combined['premis_cd'].map(premis_id_mapping)
    df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd'].map(weapon_id_mapping)
    df_combined['status_id'] = df_combined['status'].map(status_id_mapping)
    df_combined['vict_sex_id'] = df_combined['vict_sex'].map(sex_id_mapping)

    session.close()

    # Now, remove the original code columns as they are replaced by their corresponding IDs
    df_combined = df_combined.drop(columns=['area', 'crm_cd', 'premis_cd', 'weapon_used_cd', 'status', 'vict_sex'])

    # # List of columns to set NaN values to None
    # columns_to_replace_nan = ['area_id', 'crm_cd_id', 'premis_cd_id', 'weapon_used_cd_id', 'status_id', 'vict_sex_id']

    # # Loop through each column and replace NaN with None
    # for column in columns_to_replace_nan:
    #     df_combined[column] = df_combined[column].where(df_combined[column].notna(), None)

    # # Replace NaN with None in foreign key columns
    # fk_columns = ['area_id', 'crm_cd_id', 'premis_cd_id', 'weapon_used_cd_id', 'status_id', 'vict_sex_id']
    # for column in fk_columns:
    #     df_combined[column] = df_combined[column].where(pd.notnull(df_combined[column]), None)

    # After replacing NaN with None, attempt to convert to integer (or keep as None for NULL in SQL)
    # for column in fk_columns:
    #     # Using a lambda function to leave None intact while converting valid values to integers
    #     df_combined[column] = df_combined[column].apply(lambda x: int(x) if pd.notnull(x) else None)

    # Replace NaN with None for 'premis_cd_id' and 'weapon_used_cd_id'
    # df_combined['premis_cd_id'] = df_combined['premis_cd_id'].where(pd.notnull(df_combined['premis_cd_id']), None)
    # df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd_id'].where(pd.notnull(df_combined['weapon_used_cd_id']), None)

    # Convert to integers, but leave None values intact
    # df_combined['premis_cd_id'] = df_combined['premis_cd_id'].apply(lambda x: int(x) if pd.notnull(x) else None)
    # df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd_id'].apply(lambda x: int(x) if pd.notnull(x) else None)

    # Explicitly attempt to convert foreign key columns to numeric, coercing errors
    fk_columns = ['area_id', 'crm_cd_id', 'premis_cd_id', 'weapon_used_cd_id', 'status_id', 'vict_sex_id']
    for column in fk_columns:
        df_combined[column] = pd.to_numeric(df_combined[column], errors='coerce').apply(lambda x: None if pd.isna(x) else int(x))

    df_combined['premis_cd_id'] = df_combined['premis_cd_id'].apply(lambda x: None if pd.isna(x) else int(x))
    df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd_id'].apply(lambda x: None if pd.isna(x) else int(x))

    # Assuming df is your DataFrame
    df_combined['premis_cd_id'] = df_combined['premis_cd_id'].apply(lambda x: None if pd.isnull(x) else x)
    df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd_id'].apply(lambda x: None if pd.isnull(x) else x)

    # Optionally, ensure integer data type if column might contain numbers (this is optional and context-dependent, especially if you're already setting None for NaNs)
    df_combined['premis_cd_id'] = df_combined['premis_cd_id'].astype('Int64')  # 'Int64' (capital I) can handle None values
    df_combined['weapon_used_cd_id'] = df_combined['weapon_used_cd_id'].astype('Int64')  # 'Int64' (capital I) can handle None values

    df_combined['datetime_occurred'] = pd.to_datetime(df_combined['datetime_occurred'], errors='coerce')
    df_combined['datetime_occurred'] = df_combined['datetime_occurred'].where(df_combined['datetime_occurred'].notnull(), None)

    # Double-check the data types after conversion
    print(df_combined.dtypes)

    # Save the combined DataFrame
    df_combined.to_parquet('../data/processed/incidents_df.parquet', index=False)

if __name__ == "__main__":
    main()