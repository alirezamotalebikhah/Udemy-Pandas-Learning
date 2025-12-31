import pandas as pd
from sqlalchemy import create_engine, text

db_url = "postgresql+psycopg2://udemy:udemy123@localhost:5434/udemy_master_of_pandas"
engine = create_engine(db_url)

csv_file = "employees.csv"
df = pd.read_csv(csv_file)

schema_name = 'master'
table_name = 'employees'

with engine.connect() as conn:
    conn.execute(text(f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}"'))
    conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema_name}"'))
    conn.commit()

with engine.connect() as conn:
    conn.execute(text(f'CREATE SCHEMA "{schema_name}"'))
    conn.commit()

def map_dtype_to_postgres(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'NUMERIC'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'

columns_with_types = []
for col, dtype in zip(df.columns, df.dtypes):
    col_type = map_dtype_to_postgres(dtype)
    col_name = f'"{col}"'
    columns_with_types.append(f"{col_name} {col_type}")

columns_sql = ", ".join(columns_with_types)
create_table_sql = f'CREATE TABLE "{schema_name}"."{table_name}" ({columns_sql})'

with engine.connect() as conn:
    conn.execute(text(create_table_sql))
    conn.commit()

for idx, row in df.iterrows():
    cols = ", ".join([f'"{col}"' for col in df.columns])
    vals = ", ".join([f"'{str(val).replace(chr(39), chr(39)+chr(39))}'" if pd.notna(val) else 'NULL' for val in row])
    insert_sql = f'INSERT INTO "{schema_name}"."{table_name}" ({cols}) VALUES ({vals})'
    
    with engine.connect() as conn:
        conn.execute(text(insert_sql))
        conn.commit()

print(f"جدول {schema_name}.{table_name} با موفقیت ایجاد و پر شد!")

with engine.connect() as conn:
    result = conn.execute(text(f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT 5'))
    print("\n5 سطر اول:")
    for row in result:
        print(row)