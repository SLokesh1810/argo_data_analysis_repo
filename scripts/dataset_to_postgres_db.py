from sqlalchemy import create_engine, inspect, text, select, MetaData, Index, Table, Column, Integer, BigInteger, Float, TIMESTAMP, String, Boolean, Text
import pandas as pd
import os
import hashlib
import json

from dataset_vectordb import store_vector_db

Meta = MetaData()

profile_table = Table(
    "argo_profiles",
    Meta,
    Column("float_id", String(32), primary_key=True),
    Column("num_profiles", Integer),
    Column("num_rows", Integer),
    Column("datestart", TIMESTAMP),
    Column("dateend", TIMESTAMP),
    Column("latmin", Float),
    Column("latavg", Float),
    Column("latmax", Float),
    Column("lonmin", Float),
    Column("lonavg", Float),
    Column("lonmax", Float),
    Column("tempmin", Float),
    Column("tempavg", Float),
    Column("tempmax", Float),
    Column("salinitymin", Float),
    Column("salinityavg", Float),
    Column("salinitymax", Float),
    Column("depthmax", Float),
    Column("has_oxygen", Boolean),
    Column("has_chlorophyll", Boolean),
    Column("has_backscatter", Boolean),
    Column("has_nitrate", Boolean),
    Column("has_ph", Boolean),
    Column("summary", Text),
    Column("row_hash", String(64)),
)

argo_data_table = Table(
    "argo_data",
    Meta,
    Column("id", BigInteger, primary_key=True, autoincrement=True),
    Column("float_id", String(32)),
    Column("cycle_number", Integer),
    Column("datetime", TIMESTAMP),
    Column("latitude", Float),
    Column("longitude", Float),
    Column("pressure_dbar", Float),
    Column("depth_m", Float),
    Column("temperature_c", Float),
    Column("salinity_psu", Float),
    Column("dissolved_oxygen_umol_kg", Float),
    Column("nitrate_umol_kg", Float),
    Column("ph_value", Float),
    Column("chlorophyll_a_mg_m3", Float),
    Column("backscatter_700nm_m_1", Float),
)

Index(
    "idx_argo_data_float_id",
    argo_data_table.c.float_id
)

Index(
    "idx_argo_data_float_cycle",
    argo_data_table.c.float_id,
    argo_data_table.c.cycle_number
)

def create_metadata_hash(row):
    clean_row = {}

    for key, value in row.items():
        if pd.isna(value):
            clean_row[key] = None
        elif isinstance(value, pd.Timestamp):
            clean_row[key] = value.isoformat()
        else:
            clean_row[key] = value

    row_string = json.dumps(
        clean_row,
        sort_keys=True
    )

    return hashlib.md5(
        row_string.encode()
    ).hexdigest()

def get_updated_floats_meta(df: pd.DataFrame, db_url: str):
    engine = create_engine(db_url)

    with engine.begin() as conn:
        profile_table.create(engine, checkfirst=True)
        db_df = pd.read_sql(
            text("""
                SELECT float_id, row_hash
                FROM argo_profiles
            """),
            conn
        )

    if db_df.empty:
        return set(df["float_id"].tolist())

    db_hash_map = dict(zip(db_df["float_id"], db_df["row_hash"]))
    updated_floats = set()

    for row in df.itertuples(index=False):
        float_id = row.float_id
        db_hash = db_hash_map.get(float_id)
        current_hash = row.row_hash
        if db_hash != current_hash:
            updated_floats.add(float_id)
    return updated_floats

def store_metadata_in_postgres(df: pd.DataFrame, db_url: str, chunk_size: int = 100):
    # Create hashes
    df["row_hash"] = df.apply(create_metadata_hash, axis=1)

    updated_floats = get_updated_floats_meta(df, db_url)
    updated_df = df[df["float_id"].isin(updated_floats)]

    if updated_df.empty:
        return 0

    engine = create_engine(db_url)

    with engine.begin() as conn:
        # Create table if not exists
        profile_table.create(conn, checkfirst=True)

        # Delete old metadata
        conn.execute(
            text("""
                DELETE FROM argo_profiles
                WHERE float_id = ANY(:float_ids)
            """),
            {"float_ids": list(updated_floats)}
        )
        # -----------------------------
        # Chunked inserts
        # -----------------------------
        total_rows = len(updated_df)

        for start in range(0, total_rows, chunk_size):
            end = start + chunk_size
            chunk_df = updated_df.iloc[start:end]
            chunk_df.to_sql(
                "argo_profiles",
                conn,
                if_exists="append",
                index=False,
                method="multi"
            )
    
    store_vector_db(updated_df, updated_floats)

    return len(updated_floats)

def get_updated_data_floats(db_url: str) -> set:
    engine = create_engine(db_url)

    query = text("""
        SELECT p.float_id
        FROM argo_profiles p
        LEFT JOIN (
            SELECT
                float_id,
                COUNT(*) AS db_num_rows
            FROM argo_data
            GROUP BY float_id
        ) d
        ON p.float_id = d.float_id
        WHERE d.db_num_rows IS NULL
        OR p.num_rows != d.db_num_rows
    """)

    with engine.connect() as conn:
        result = conn.execute(query)
        updated_floats = {
            row.float_id
            for row in result
        }
    
    return updated_floats

def get_all_floats(db_url: str):
    engine = create_engine(db_url)
    
    select_statement = select(profile_table.c.float_id)
    
    with engine.connect() as conn:
        result = conn.execute(select_statement)
        float_ids = result.scalars().all()

    return float_ids

def store_data_in_postgres(data_path: str, db_url: str, chunk_size: int = 100000):
    engine = create_engine(db_url)

    with engine.begin() as conn:
        table_exists = inspect(conn).has_table("argo_data")
        argo_data_table.create(conn, checkfirst=True)

        if table_exists:
            updated_floats = get_updated_data_floats(db_url)
        else:
            updated_floats = get_all_floats(db_url)

        total_inserted = 0

        for float_id in updated_floats:
            parquet_path = os.path.join(data_path, f"{float_id}.parquet")

            if not os.path.exists(parquet_path):
                print(f"Missing parquet: {float_id}")
                continue

            data = pd.read_parquet(parquet_path)
            total_rows = len(data)

            # Remove old rows for updated floats
            delete_statement = argo_data_table.delete().where(argo_data_table.c.float_id == float_id)
            conn.execute(delete_statement)

            for start in range(0, total_rows, chunk_size):
                end = min(start + chunk_size, total_rows)
                chunk_df = data.iloc[start:end]

                chunk_df.to_sql(
                    "argo_data",
                    conn,
                    if_exists="append",
                    index=False,
                    method='multi'
                )

                total_inserted += len(chunk_df)
            
    return total_inserted, len(updated_floats)