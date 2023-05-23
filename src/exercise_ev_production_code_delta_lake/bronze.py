from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp, year, month, dayofmonth, hour, minute, concat, lpad


class Bronze:

    def set_partitioning_cols(self, input_df: DataFrame):
        return input_df. \
            withColumn("year", year(col("write_timestamp"))). \
            withColumn("month", month(col("write_timestamp"))). \
            withColumn("day", dayofmonth(col("write_timestamp"))). \
            withColumn("hour", hour(col("write_timestamp"))). \
            withColumn("minute", minute(col("write_timestamp")))
