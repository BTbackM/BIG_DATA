from glob import glob
from os import path

import pandas as pd

ABS_PATH = path.dirname(path.abspath(__file__))

headers = {
    'part'      : ['PARTKEY', 'NAME', 'MFGR', 'BRAND', 'TYPE', 'SIZE', 'CONTAINER', 'RETAILPRICE', 'COMMENT'],
    'supplier'  : ["SUPPKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "COMMENT"],
    'partsupp'  : ["PARTKEY", "SUPPKEY", "AVAILQTY", "SUPPLYCOST", "COMMENT"],
    'customer'  : ["CUSTKEY", "NAME", "ADDRESS", "NATIONKEY", "PHONE", "ACCTBAL", "MKTSEGMENT", "COMMENT"],
    'nation'    : ["NATIONKEY", "NAME", "REGIONKEY", "COMMENT"],
    'lineitem'  : ["ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY",
                   "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE",
                   "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"],
    'region'    : ["REGIONKEY", "NAME", "COMMENT"],
    'orders'    : ["ORDERKEY", "CUSTKEY", "ORDERSTATUS", "TOTALPRICE", "ORDERDATE", "ORDERPRIORITY", "CLERK", "SHIPPRIORITY", "COMMENT"],
}

data_path = path.join(ABS_PATH, 'data')

# Convert files in data folder(tbl) to parquet files
def from_tbl_to_parquet():
    # Source path
    tbl_path = path.join(data_path, 'tbl')
    # Destination path
    parquet_path = path.join(data_path, 'parquet')

    files = glob(path.join(tbl_path, '*.tbl'))
    for file in files:
        # Get table name
        table_name = path.splitext(path.basename(file))[0]
        # Read file
        df = pd.read_csv(file, sep='|', names=headers[table_name])
        # Write to parquet file
        df.to_parquet(path.join(parquet_path, table_name + '.parquet'))

def main():
    from_tbl_to_parquet()

if __name__ == '__main__':
    main()
