from glob import glob
from os import path

import pandas as pd
import duckdb as db

ABS_PATH = path.dirname(path.abspath(__file__))
parquet_path = path.join(ABS_PATH, '../WEEK_04/data/parquet')

def read_parquet(name):
    return pd.read_parquet(path.join(parquet_path, name))

def queries():
    # Read parquet files
    customer = read_parquet('customer.parquet')
    lineitem = read_parquet('lineitem.parquet')
    nation = read_parquet('nation.parquet')
    orders = read_parquet('orders.parquet')
    part = read_parquet('part.parquet')
    partsupp = read_parquet('partsupp.parquet')
    region = read_parquet('region.parquet')
    supplier = read_parquet('supplier.parquet')

    # Query 1
    query_1 = """
    select
        sum(l_extendedprice * l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= date '1994-01-01'
        and l_shipdate < date '1994-01-01' + interval '1' year
        and l_discount between .06 - 0.01 and .06 + 0.01
        and l_quantity < 24;
    """
    print(lineitem.columns)
    # output = db.sql(query_1)
    # print(output)
    # print(tables['lineitem'].head())
    print(query_1)

def main():
    queries()

if __name__ == '__main__':
    main()
