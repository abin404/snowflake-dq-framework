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



def run_dq_checks(session, table_name, dq_rules):
    df = session.table(table_name)
    results = []

    for rule in dq_rules:
        dq_type = rule["dq_type"]
        col = rule.get("column")
        params = rule.get("params", {})

        dq_func = DQ_FUNCTIONS.get(dq_type)
        if not dq_func:
            print(f"⚠️ Unknown rule: {dq_type}")
            continue

        try:
            if dq_type == "referential_check":
                res = dq_func(session, df, **params)
            elif dq_type == "cross_field_check":
                res = dq_func(df, params["col1"], params["operator"], params["col2"])
            else:
                res = dq_func(df, col, **params)

            res.update({
                "table": table_name,
                "dq_type": dq_type,
                "column": col,
                "status": "PASS" if res["percent"] == 0 else "FAIL"
            })
            results.append(res)
        except Exception as e:
            results.append({
                "table": table_name,
                "dq_type": dq_type,
                "column": col,
                "status": "ERROR",
                "error": str(e)
            })

    return session.create_dataframe(results)
