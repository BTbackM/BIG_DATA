from glob import glob
from os import path

import duckdb as db
import pandas as pd
import time

ABS_PATH = path.dirname(path.abspath(__file__))
parquet_path = path.join(ABS_PATH, '../WEEK_04/data/parquet')
benchmark_path = path.join(ABS_PATH, '../WEEK_07/LAB_07/src/benchmark_queries')
results_path = path.join(ABS_PATH, '../WEEK_07/LAB_07/src/expected_results')
duckdb_times = path.join(ABS_PATH, '../WEEK_07/LAB_07/data/duckdb_times.csv')

def read_parquet(name):
    return pd.read_parquet(path.join(parquet_path, name))

def read_benchmark(name):
    with open(path.join(benchmark_path, name)) as f:
        return f.read()

def make_query(query, name):
    start = time.time()
    output = db.sql(query)
    end = time.time()
    with open(duckdb_times, 'a') as f:
        f.write(f'{name},{(end - start) * 1000}\n')
    # Results to CSV
    column_names = [desc[0] for desc in output.description]
    df = pd.DataFrame(output.fetchall(), columns=column_names)
    df.to_csv(path.join(results_path, f'{name:02d}.csv'), index=False)

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

    # Do every query
    for i in range(1, 23):
        try:
            print(f'Query {i:02d}')
            query = read_benchmark(f'{i:02d}.sql')
            for _ in range(20):
                make_query(query, i)
        except Exception as e:
            print(f'Query {i:02d} failed: {e}')

def main():
    queries()

if __name__ == '__main__':
    main()
