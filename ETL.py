import logging
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import pandas as pd

NAME_APP = 'tset_doofi'

logger = logging.getLogger(NAME_APP)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

spark = SparkSession.builder.appName(NAME_APP).getOrCreate()


def extract(path):
    base_path = "s3://ifood-data-architect-test-source/"
    full_path = base_path + path
    logger.info(f"Downloading: {full_path}")

    path_split = path.split('.')
    fmt = path_split[-2] if path_split[-1] in ('gz', 'gzip', 'zip') else path_split[-1]

    kwargs = {"header": True, "inferSchema": True} if fmt == "csv" else {}

    return spark.read.load(full_path, format=fmt, **kwargs)

def transform(df_order):
    col_order = 'OrderNumberForThisCustomer'
    window_spec = Window.partitionBy(df_order.order_id).orderBy(df_order.order_created_at.desc())
    df_order_row = df_order.withColumn(col_order, row_number().over(window_spec))
    colunas = [c for c in df_order_row.columns if c != col_order]
    df_order_dedup = df_order_row.where(f"{col_order} = 1").select(colunas)

    #show_info(df_order_dedup, 'df_order_dedup')

def main():
    df_order = transform(extract(path="order.json.gz").cache())
    df_restaurant = extract(path="restaurant.csv.gz").cache()
    df_consumer = extract(path="consumer.csv.gz").cache()

if __name__ == '__main__':
    main()