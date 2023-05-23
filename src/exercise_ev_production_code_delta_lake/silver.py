from pyspark.sql import DataFrame
from pyspark.sql.functions import from_json, col, explode, to_timestamp, round
from pyspark.sql.types import StructType, StringType, StructField, ArrayType, IntegerType, DoubleType


class MeterValuesRequestTransformer:

    def _meter_values_request_filter(self, input_df: DataFrame):
        action = "MeterValues"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _meter_values_request_unpack_json(self, input_df: DataFrame):
        sampled_value_schema = StructType([
            StructField("value", StringType()),
            StructField("context", StringType()),
            StructField("format", StringType()),
            StructField("measurand", StringType()),
            StructField("phase", StringType()),
            StructField("unit", StringType()),
        ])

        meter_value_schema = StructType([
            StructField("timestamp", StringType()),
            StructField("sampled_value", ArrayType(sampled_value_schema)),
        ])

        body_schema = StructType([
            StructField("connector_id", IntegerType()),
            StructField("transaction_id", IntegerType()),
            StructField("meter_value", ArrayType(meter_value_schema)),
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _meter_values_request_flatten(self, input_df: DataFrame):
        return input_df. \
            select("*", explode("new_body.meter_value").alias("meter_value")). \
            select("*", explode("meter_value.sampled_value").alias("sampled_value")). \
            withColumn("timestamp", to_timestamp(col("meter_value.timestamp"))). \
            withColumn("measurand", col("sampled_value.measurand")). \
            withColumn("phase", col("sampled_value.phase")). \
            withColumn("value", round(col("sampled_value.value").cast(DoubleType()), 2)). \
            select("message_id", "message_type", "charge_point_id", "action", "write_timestamp",
                   col("new_body.transaction_id").alias("transaction_id"),
                   col("new_body.connector_id").alias("connector_id"), "timestamp", "measurand", "phase", "value")

    def run(self, input_df: DataFrame) -> DataFrame:
        return input_df. \
             transform(self._meter_values_request_filter). \
             transform(self._meter_values_request_unpack_json). \
             transform(self._meter_values_request_flatten)


class StartTransactionRequestTransformer:
    def _start_transaction_request_filter(self, input_df: DataFrame):
        action = "StartTransaction"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _start_transaction_request_unpack_json(self, input_df: DataFrame):
        body_schema = StructType([
            StructField("connector_id", IntegerType(), True),
            StructField("id_tag", StringType(), True),
            StructField("meter_start", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("reservation_id", IntegerType(), True),
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _start_transaction_request_flatten(self, input_df: DataFrame):
        return input_df.\
            withColumn("connector_id", input_df.new_body.connector_id).\
            withColumn("id_tag", input_df.new_body.id_tag).\
            withColumn("meter_start", input_df.new_body.meter_start).\
            withColumn("timestamp", input_df.new_body.timestamp).\
            withColumn("reservation_id", input_df.new_body.reservation_id).\
            drop("new_body").\
            drop("body")

    def _start_transaction_request_cast(self, input_df: DataFrame) -> DataFrame:
        return input_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    def run(self, input_df: DataFrame) -> DataFrame:
        return input_df. \
            transform(self._start_transaction_request_filter). \
            transform(self._start_transaction_request_unpack_json). \
            transform(self._start_transaction_request_flatten). \
            transform(self._start_transaction_request_cast)

class StartTransactionResponseTransformer:

    def _start_transaction_response_filter(self, input_df: DataFrame):
        action = "StartTransaction"
        message_type = 3
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _start_transaction_response_unpack_json(self, input_df: DataFrame):
        id_tag_info_schema = StructType([
            StructField("status", StringType(), True),
            StructField("parent_id_tag", StringType(), True),
            StructField("expiry_date", StringType(), True),
        ])

        body_schema = StructType([
            StructField("transaction_id", IntegerType(), True),
            StructField("id_tag_info", id_tag_info_schema, True)
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _start_transaction_response_flatten(self, input_df: DataFrame):
        return input_df. \
            withColumn("transaction_id", input_df.new_body.transaction_id). \
            withColumn("id_tag_info_status", input_df.new_body.id_tag_info.status). \
            withColumn("id_tag_info_parent_id_tag", input_df.new_body.id_tag_info.parent_id_tag). \
            withColumn("id_tag_info_expiry_date", input_df.new_body.id_tag_info.expiry_date). \
            drop("new_body"). \
            drop("body")

    def run(self, input_df: DataFrame) -> DataFrame:
       return input_df. \
           transform(self._start_transaction_response_filter). \
           transform(self._start_transaction_response_unpack_json). \
           transform(self._start_transaction_response_flatten)


class StopTransactionRequestTransformer:

    def _stop_transaction_request_filter(self, input_df: DataFrame):
        action = "StopTransaction"
        message_type = 2
        return input_df.filter((input_df.action == action) & (input_df.message_type == message_type))

    def _stop_transaction_request_unpack_json(self, input_df: DataFrame):
        body_schema = StructType([
            StructField("meter_stop", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("transaction_id", IntegerType(), True),
            StructField("reason", StringType(), True),
            StructField("id_tag", StringType(), True),
            StructField("transaction_data", ArrayType(StringType()), True)
        ])
        return input_df.withColumn("new_body", from_json(col("body"), body_schema))

    def _stop_transaction_request_flatten(self, input_df: DataFrame):
        return input_df. \
            withColumn("meter_stop", input_df.new_body.meter_stop). \
            withColumn("timestamp", input_df.new_body.timestamp). \
            withColumn("transaction_id", input_df.new_body.transaction_id). \
            withColumn("reason", input_df.new_body.reason). \
            withColumn("id_tag", input_df.new_body.id_tag). \
            withColumn("transaction_data", input_df.new_body.transaction_data). \
            drop("new_body"). \
            drop("body")

    def _stop_transaction_request_cast(self, input_df: DataFrame) -> DataFrame:
        return input_df.withColumn("timestamp", to_timestamp(col("timestamp")))

    def run(self, input_df: DataFrame):
        return input_df. \
            transform(self._stop_transaction_request_filter). \
            transform(self._stop_transaction_request_unpack_json). \
            transform(self._stop_transaction_request_flatten). \
            transform(self._stop_transaction_request_cast)