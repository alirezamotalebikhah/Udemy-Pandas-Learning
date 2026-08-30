import os
import pandas as pd
from sqlalchemy import create_engine, text


# =========================
# Database configuration
# =========================

db_url = "postgresql+psycopg2://udemy:udemy123@localhost:5434/udemy_master_of_pandas"
engine = create_engine(db_url)

schema_name = "archive"
csv_directory = "."


# =========================
# PostgreSQL type mapping
# =========================


def map_dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"

    elif pd.api.types.is_float_dtype(dtype):
        return "NUMERIC"

    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"

    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"

    else:
        return "TEXT"


# =========================
# Create fresh schema
# =========================

with engine.begin() as conn:
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE'))

    conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))


# =========================
# Process CSV files
# =========================

for filename in os.listdir(csv_directory):
    # Only process CSV files
    if not filename.endswith(".csv"):
        continue

    csv_path = os.path.join(csv_directory, filename)

    # -------------------------
    # Table name
    # -------------------------

    table_name = os.path.splitext(filename)[0]

    print(f"\nProcessing: {filename}")
    print(f"Table: {schema_name}.{table_name}")

    # -------------------------
    # Read CSV
    # -------------------------

    df = pd.read_csv(csv_path)

    # -------------------------
    # Create column definitions
    # -------------------------

    columns_with_types = []

    for col, dtype in zip(df.columns, df.dtypes):
        col_type = map_dtype_to_postgres(dtype)

        col_name = f'"{col}"'

        columns_with_types.append(f"{col_name} {col_type}")

    columns_sql = ", ".join(columns_with_types)

    # -------------------------
    # Create table
    # -------------------------

    create_table_sql = f'''
        CREATE TABLE "{schema_name}"."{table_name}"
        ({columns_sql})
    '''

    with engine.begin() as conn:
        conn.execute(text(create_table_sql))

    # -------------------------
    # Insert data
    # -------------------------

    for _, row in df.iterrows():
        cols = ", ".join(f'"{col}"' for col in df.columns)

        vals = ", ".join(
            [
                f"'{str(val).replace(chr(39), chr(39) + chr(39))}'"
                if pd.notna(val)
                else "NULL"
                for val in row
            ]
        )

        insert_sql = f'''
            INSERT INTO "{schema_name}"."{table_name}"
            ({cols})
            VALUES ({vals})
        '''

        with engine.begin() as conn:
            conn.execute(text(insert_sql))

    print(f"✓ {schema_name}.{table_name} created successfully ({len(df)} rows)")


# =========================
# Show created tables
# =========================

with engine.connect() as conn:
    result = conn.execute(
        text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            ORDER BY table_name
        """),
        {"schema": schema_name},
    )

    print("\nCreated tables:")

    for row in result:
        print(f"  - {schema_name}.{row[0]}")


print("\n✓ All CSV files imported successfully!")
