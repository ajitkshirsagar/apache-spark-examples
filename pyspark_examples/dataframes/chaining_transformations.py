"""
Following code snippet demonstrates on how we can define custom transformations & chain the function calls
since, Spark Scala API provides `.transform` method for DataFrame through which we can chain the function calls
but PySpark's DataFrame API lacks this in Spark version < 3.x
https://github.com/apache/spark/pull/23877
"""
import typing as T
from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.types as spark_types
import pyspark.sql.functions as F


def transform(self, f) -> DataFrame:
    return f(self)


DataFrame.transform = transform


def create_dataframe(spark: SparkSession) -> DataFrame:
    data = [
        {'idx': 1, 'idy': 5},
        {'idx': 2, 'idy': 6},
        {'idx': 3, 'idy': 7},
        {'idx': 4, 'idy': 8},
    ]

    schema = spark_types.StructType([
        spark_types.StructField('idx', spark_types.IntegerType(), True),
        spark_types.StructField('idy', spark_types.StringType(), True)
    ])
    return spark.createDataFrame(data, schema)


def sum_col(df: DataFrame) -> DataFrame:
    return df.withColumn('sum', (F.col('idx') + F.col('idy')).cast('integer'))


def square_col(df: DataFrame) -> DataFrame:
    return df.withColumn('squared_sum', (F.col('sum') * F.col('sum')).cast('long'))


def sum_idx_idy() -> T.Callable[[DataFrame], DataFrame]:
    def inner(df: DataFrame) -> DataFrame:
        return df.withColumn(
            'sum', (F.col('idx') + F.col('idy')).cast('integer')
        )
    return inner


def square_sum_idx_idy() -> T.Callable[[DataFrame], DataFrame]:
    def inner(df: DataFrame) -> DataFrame:
        return df.withColumn(
            'squared_sum', (F.col('sum') * F.col('sum')).cast('long')
        )
    return inner


def main():

    spark = SparkSession\
        .builder\
        .appName('chaining transformations')\
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    df = create_dataframe(spark=spark)

    # Conventional way of writing transformations as function calls
    sum_df = sum_col(df)
    squared_df = square_col(sum_df)
    squared_df.show()

    # Best practice: Chaining the function calls using .transform
    squared_df_1 = df.transform(sum_idx_idy()).transform(square_sum_idx_idy())
    squared_df_1.show()


if __name__ == "__main__":
    main()

