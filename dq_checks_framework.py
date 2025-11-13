from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from datetime import datetime, date, timedelta
from enum import Enum


# ------------------------------------------------------------------------------
# ENUM DEFINITIONS
# ------------------------------------------------------------------------------
class DQStatus(str, Enum):
    PASS_ = "PASS"
    FAIL = "FAIL"
    CHECK = "CHECK"


class DQDimension(str, Enum):
    UNIQUENESS = "Uniqueness"
    COMPLETENESS = "Completeness"
    RECONCILIATION = "Reconciliation"
    TIMELINESS = "Timeliness"


# ------------------------------------------------------------------------------
# UTILITIES
# ------------------------------------------------------------------------------
def current_timestamp():
    """Return UTC timestamp string."""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def create_result(dimension, column_name, status, failed_rows, edi_business_day, dq_comment):
    """Helper to build one flattened DQ record."""
    return {
        "audit_timestamp": current_timestamp(),
        "dq_dimension": dimension.value,
        "column_name": column_name,
        "status": status.value,
        "failed_rows": failed_rows,
        "edi_business_day": edi_business_day,
        "dq_comment": dq_comment,
    }


# ------------------------------------------------------------------------------
# 1️⃣ Uniqueness Check
# ------------------------------------------------------------------------------
def check_uniqueness(df, primary_key_columns, edi_business_day):
    if not primary_key_columns:
        return []

    dup_count = (
        df.groupBy(*primary_key_columns)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status = DQStatus.PASS_ if dup_count == 0 else DQStatus.FAIL
    dq_comment = f"Duplicate key rows found: {dup_count}"

    return [create_result(DQDimension.UNIQUENESS, ",".join(primary_key_columns),
                          status, dup_count, edi_business_day, dq_comment)]


# ------------------------------------------------------------------------------
# 2️⃣ Completeness Check (NOT NULL)
# ------------------------------------------------------------------------------
def check_completeness(df, not_null_columns, edi_business_day):
    results = []
    for col in not_null_columns or []:
        null_count = df.filter(F.col(col).isNull()).count()
        status = DQStatus.PASS_ if null_count == 0 else DQStatus.FAIL
        dq_comment = f"Null values in {col}: {null_count}"
        results.append(create_result(DQDimension.COMPLETENESS, col,
                                     status, null_count, edi_business_day, dq_comment))
    return results


# ------------------------------------------------------------------------------
# 3️⃣ Reconciliation Check (Source vs Target)
# ------------------------------------------------------------------------------
def check_reconciliation(df_source, df_target, value_column, edi_business_day):
    if not value_column:
        return []

    src_count = df_source.count()
    tgt_count = df_target.count()

    src_sum = df_source.agg(F.sum(F.col(value_column))).collect()[0][0] or 0.0
    tgt_sum = df_target.agg(F.sum(F.col(value_column))).collect()[0][0] or 0.0

    count_status = DQStatus.PASS_ if src_count == tgt_count else DQStatus.FAIL
    sum_status = DQStatus.PASS_ if src_sum == tgt_sum else DQStatus.FAIL

    return [
        create_result(DQDimension.RECONCILIATION, value_column,
                      count_status, abs(src_count - tgt_count), edi_business_day,
                      f"Count check — Source: {src_count}, Target: {tgt_count}"),
        create_result(DQDimension.RECONCILIATION, value_column,
                      sum_status, abs(src_sum - tgt_sum), edi_business_day,
                      f"Sum check — Source: {src_sum}, Target: {tgt_sum}")
    ]


# ------------------------------------------------------------------------------
# 4️⃣ Timeliness Check
# ------------------------------------------------------------------------------
def check_timeliness(df, edi_business_day, expected_lag_days=2):
    if "edi_business_day" not in df.columns:
        return []

    max_date = df.agg(F.max(F.col("edi_business_day"))).collect()[0][0]
    if not max_date:
        return []

    expected_min_date = date.today() - timedelta(days=expected_lag_days)
    lag_days = (date.today() - max_date).days
    status = DQStatus.PASS_ if max_date >= expected_min_date else DQStatus.FAIL
    dq_comment = f"Latest date: {max_date}, lag days: {lag_days}"

    return [create_result(DQDimension.TIMELINESS, "edi_business_day",
                          status, lag_days, edi_business_day, dq_comment)]


# ------------------------------------------------------------------------------
# 5️⃣ Orchestrator
# ------------------------------------------------------------------------------
def run_dq_checks(
    df_source,
    df_target,
    edi_business_day,
    primary_key_columns=None,
    not_null_columns=None,
    value_column=None,
    expected_lag_days=2,
):
    """
    Executes all DQ checks and returns a LIST of results.
    """

    dq_results = []
    dq_results += check_uniqueness(df_target, primary_key_columns, edi_business_day)
    dq_results += check_completeness(df_target, not_null_columns, edi_business_day)
    dq_results += check_reconciliation(df_source, df_target, value_column, edi_business_day)
    dq_results += check_timeliness(df_target, edi_business_day, expected_lag_days)

    return dq_results


# ------------------------------------------------------------------------------
# 6️⃣ Write Function — Write DQ Results to S3 as Parquet
# ------------------------------------------------------------------------------
def write_dq_results(spark, dq_results, s3_path):
    """
    Converts list of DQ results into a Spark DataFrame and writes to S3 as Parquet.

    Args:
        spark: SparkSession
        dq_results: list of dicts returned from run_dq_checks()
        s3_path: str - S3 location (e.g., "s3://bucket/path/to/audit")
    """

    schema = StructType([
        StructField("audit_timestamp", StringType(), False),
        StructField("dq_dimension", StringType(), False),
        StructField("column_name", StringType(), True),
        StructField("status", StringType(), False),
        StructField("failed_rows", IntegerType(), False),
        StructField("edi_business_day", StringType(), False),
        StructField("dq_comment", StringType(), True)
    ])

    dq_df = spark.createDataFrame(dq_results, schema=schema)
    dq_df.write.mode("append").parquet(s3_path)
    return dq_df


# inputs[0] = source DataFrame
# inputs[1] = target DataFrame

dq_results = run_dq_checks(
    df_source=inputs[0],
    df_target=inputs[1],
    edi_business_day=parameters["edi_business_day"],
    primary_key_columns=parameters.get("primary_key_columns", ["order_id"]),
    not_null_columns=parameters.get("not_null_columns", ["customer_id", "amount"]),
    value_column=parameters.get("value_column", "amount"),
    expected_lag_days=int(parameters.get("expected_lag_days", 2))
)

# write results to S3
spark = inputs[0].sql_ctx.sparkSession
dq_df = write_dq_results(spark, dq_results, parameters["dq_audit_path"])

output = dq_df  # optional, can be used in next StreamSets stage
