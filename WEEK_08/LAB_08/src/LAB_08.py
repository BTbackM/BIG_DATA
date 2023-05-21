from os import path

import pyarrow as pa
import pyarrow.parquet as pq

# Global variables
ABS_PATH = path.dirname(path.abspath(__file__))
parquet_path = path.join(ABS_PATH, '../../../WEEK_04/data/parquet')
partitioned_path = path.join(ABS_PATH, '../data/partitioned_parquet')

# Partition columns for each table
partition_cols = {
    'customer': ['c_nationkey'],
    'lineitem': ['l_shipmode'],
    'nation': [],
    'orders': ['o_orderstatus'],
    'part': ['p_brand'],
    'partsupp': [],
    'region': [],
    'supplier': ['s_nationkey']
}

# Partition tables using pyarrow and partition_cols
def partition_table(table_name):
    table_path = path.join(parquet_path, f'{table_name}.parquet')
    table = pq.read_table(table_path)
    # Write partitioned tables to partitioned_path
    pq.write_to_dataset(
        table,
        root_path = partitioned_path,
        partition_cols = partition_cols[table_name]
    )

# Partition all tables
def main():
    for table_name in partition_cols:
        print(f'Partitioning {table_name}...')
        partition_table(table_name)

if __name__ == '__main__':
    main()
