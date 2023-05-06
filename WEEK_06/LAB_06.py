from glob import glob
from os import path

import pandas as pd
import duckdb as db

ABS_PATH = path.dirname(path.abspath(__file__))
parquet_path = path.join(ABS_PATH, '../WEEK_04/data/parquet')

def queries():
    # Read parquet files
    files = glob(path.join(parquet_path, '*.parquet'))
    tables = {}
    for file in files:
        # Get table name
        table_name = path.splitext(path.basename(file))[0]
        # Read file
        df = pd.read_parquet(file)
        tables[table_name] = df

    # Query 1
    query_1 = f"""
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        {tables['lineitem']}
    where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24;
    """
    # output = db.sql(query_1)
    # print(output)
    print(tables['lineitem'].head())

def main():
    queries()

if __name__ == '__main__':
    main()
