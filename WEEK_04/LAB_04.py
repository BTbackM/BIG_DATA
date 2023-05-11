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

table_abbr = {
    'part'      : 'p',
    'supplier'  : 's',
    'partsupp'  : 'ps',
    'customer'  : 'c',
    'nation'    : 'n',
    'lineitem'  : 'l',
    'region'    : 'r',
    'orders'    : 'o',
}

data_path = path.join(ABS_PATH, 'data')

def get_headers(name):
    # Headers to lower case
    headers[name] = [f'{table_abbr[name]}_{header.lower()}' for header in headers[name]]
    return headers[name]

# Convert files in data folder(tbl) to parquet files
def from_tbl_to_parquet():
    # Source path
    tbl_path = path.join(data_path, 'tbl')
    # Destination path
    parquet_path = path.join(data_path, 'parquet')

    files = glob(path.join(tbl_path, '*.tbl'))
    for file in files:
        print(f'Converting {file} to parquet file')
        # Get table name
        table_name = path.splitext(path.basename(file))[0]
        # Read file
        df = pd.read_table(file, sep='|', header=None)
        df = df.iloc[:, :-1]
        df.columns = get_headers(table_name)
        # Write to parquet file
        df.to_parquet(path.join(parquet_path, table_name + '.parquet'))

def main():
    from_tbl_to_parquet()

if __name__ == '__main__':
    main()
