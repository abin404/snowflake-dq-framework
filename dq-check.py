from snowflake.snowpark import functions as F

DQ_FUNCTIONS = {}

def dq_check(name):
    """Decorator to register DQ checks dynamically."""
    def decorator(func):
        DQ_FUNCTIONS[name] = func
        return func
    return decorator


@dq_check("null_check")
def null_check(df, col):
    total = df.count()
    nulls = df.filter(F.col(col).is_null()).count()
    return {"failed": nulls, "percent": (nulls / total) * 100}

@dq_check("blank_check")
def blank_check(df, col):
    total = df.count()
    blanks = df.filter(F.trim(F.col(col)) == '').count()
    return {"failed": blanks, "percent": (blanks / total) * 100}

@dq_check("regex_check")
def regex_check(df, col, pattern):
    total = df.count()
    invalid = df.filter(~F.col(col).rlike(pattern)).count()
    return {"failed": invalid, "percent": (invalid / total) * 100}


@dq_check("range_check")
def range_check(df, col, min_val=None, max_val=None):
    total = df.count()
    cond = None
    if min_val is not None:
        cond = F.col(col) < min_val if cond is None else (cond | (F.col(col) < min_val))
    if max_val is not None:
        cond = F.col(col) > max_val if cond is None else (cond | (F.col(col) > max_val))
    failed = df.filter(cond).count() if cond is not None else 0
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("outlier_check")
def outlier_check(df, col, z_threshold=3):
    stats = df.agg(
        F.mean(F.col(col)).alias("mean"),
        F.stddev(F.col(col)).alias("stddev")
    ).collect()[0]
    mean, std = stats["mean"], stats["stddev"]
    if not std:
        return {"failed": 0, "percent": 0}
    total = df.count()
    failed = df.filter(F.abs((F.col(col) - mean) / std) > z_threshold).count()
    return {"failed": failed, "percent": (failed / total) * 100}

@dq_check("cross_field_check")
def cross_field_check(df, col1, operator, col2):
    total = df.count()
    if operator == ">":
        failed = df.filter(F.col(col1) <= F.col(col2)).count()
    elif operator == "<":
        failed = df.filter(F.col(col1) >= F.col(col2)).count()
    else:
        failed = df.filter(F.col(col1) != F.col(col2)).count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("referential_check")
def referential_check(session, df, col, ref_table, ref_col):
    ref_df = session.table(ref_table).select(ref_col).distinct()
    failed = df.join(ref_df, df[col] == ref_df[ref_col], "left_anti").count()
    total = df.count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("value_set_check")
def value_set_check(df, col, allowed_values):
    total = df.count()
    failed = df.filter(~F.col(col).isin(allowed_values)).count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("data_type_check")
def data_type_check(df, col, expected_type):
    total = df.count()
    expected_type = expected_type.upper()

    if expected_type in ["NUMBER", "INT", "INTEGER", "FLOAT", "DECIMAL"]:
        # Fail rows that contain non-numeric characters
        failed = df.filter(~F.col(col).rlike(r"^-?\d+(\.\d+)?$")).count()

    elif expected_type in ["DATE", "TIMESTAMP"]:
        # Detect invalid dates or timestamps
        failed = df.filter(F.expr(f"TRY_TO_DATE({col}) IS NULL AND {col} IS NOT NULL")).count()

    elif expected_type in ["STRING", "VARCHAR", "TEXT"]:
        # Only fail if it's purely numeric — allow date-like strings
        failed = df.filter(F.col(col).rlike(r"^\d+$")).count()

    else:
        raise ValueError(f"Unsupported expected_type: {expected_type}")

    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("length_check")
def length_check(df, col, min_len=None, max_len=None):
    total = df.count()
    cond = None
    if min_len is not None:
        cond = F.length(F.col(col)) < min_len
    if max_len is not None:
        cond = cond | (F.length(F.col(col)) > max_len) if cond is not None else F.length(F.col(col)) > max_len
    failed = df.filter(cond).count() if cond is not None else 0
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("date_validity_check")
def date_validity_check(df, col, date_format="YYYY-MM-DD"):
    total = df.count()
    failed = df.filter(F.to_date(F.col(col), date_format).is_null()).count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("duplicate_row_check")
def duplicate_row_check(df):
    total = df.count()
    failed = df.group_by(df.columns).count().filter(F.col("count") > 1).count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("custom_expression_check")
def custom_expression_check(df, expression):
    """Run custom Snowpark SQL-like filter and count failed rows."""
    total = df.count()
    failed = df.filter(~F.expr(expression)).count()
    return {"failed": failed, "percent": (failed / total) * 100}


@dq_check("conditional_check")
def conditional_check(df, condition, col, expected_value):
    """Check if a column meets expected value when condition is true."""
    total = df.filter(F.expr(condition)).count()
    failed = df.filter(F.expr(condition) & (F.col(col) != F.lit(expected_value))).count()
    return {"failed": failed, "percent": (failed / total) * 100 if total else 0}


# ---------------- DQ Runner ----------------

def run_dq_from_config(session, dq_config):
    all_results = []

    for table_entry in dq_config["tables"]:
        table_name = table_entry["name"]
        df = session.table(table_name)
        print(f"Running DQ for table: {table_name}")

        for col_entry in table_entry["columns"]:
            col = col_entry["name"]
            for rule in col_entry["dq_rules"]:
                dq_type = rule["dq_type"]
                params = rule.get("params", {})
                threshold = rule.get("threshold", 1.0)

                dq_func = DQ_FUNCTIONS.get(dq_type)
                if dq_func is None:
                    print(f"⚠️ Unknown DQ check: {dq_type}")
                    continue

                try:
                    # Allow both (df, col, **params) and (session, df, col, **params)
                    if dq_type == "referential_check":
                        result = dq_func(session, df, col, **params)
                    elif dq_type == "duplicate_row_check":
                        result = dq_func(df)
                    elif dq_type == "custom_expression_check":
                        result = dq_func(df, **params)
                    elif dq_type == "conditional_check":
                        result = dq_func(df, **params)
                    else:
                        result = dq_func(df, col, **params)

                    result.update({
                        "table": table_name,
                        "column": col,
                        "dq_type": dq_type,
                        "status": "PASS" if result["percent"] <= threshold else "FAIL"
                    })
                    all_results.append(result)
                except Exception as e:
                    all_results.append({
                        "table": table_name,
                        "column": col,
                        "dq_type": dq_type,
                        "status": "ERROR",
                        "error": str(e)
                    })

    return session.create_dataframe(all_results)
