"""Minimal stand-in for delta-spark, for machines that can't download the Delta jars.

It emulates the subset of the DeltaTable API the notebooks use (forName, alias, merge with
whenMatchedUpdate[All] / whenNotMatchedInsert[All] / whenNotMatchedBySourceUpdate, history)
on top of plain parquet tables, following Delta's MERGE semantics. `format("delta")` is
redirected to parquet and `OPTIMIZE` becomes a no-op.

It exists only so tests/local_e2e.py can check the *notebook logic* (SCD2, joins, DQ, ML)
anywhere. With real Delta available the test uses it instead (set USE_DELTA_SHIM=0).
"""
import sys
import types
from functools import reduce

from pyspark.sql import DataFrame, DataFrameWriter, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import DataStreamWriter

_METRICS: dict = {}


def _expr(s):
    return F.expr(s) if isinstance(s, str) else s


class _Merge:
    def __init__(self, table, t_alias, source: DataFrame, s_alias, cond):
        self.table, self.ta, self.src, self.sa, self.cond = table, t_alias, source, s_alias, cond
        self.matched, self.not_matched, self.nmbs = [], [], []

    # --- builder API ---------------------------------------------------------
    def whenMatchedUpdate(self, condition=None, set=None):
        self.matched.append((condition, set)); return self

    def whenMatchedUpdateAll(self, condition=None):
        self.matched.append((condition, "*")); return self

    def whenNotMatchedInsert(self, condition=None, values=None):
        self.not_matched.append((condition, values)); return self

    def whenNotMatchedInsertAll(self, condition=None):
        self.not_matched.append((condition, "*")); return self

    def whenNotMatchedBySourceUpdate(self, condition=None, set=None):
        self.nmbs.append((condition, set)); return self

    # --- execution -------------------------------------------------------------
    def execute(self):
        spark = self.src.sparkSession
        tgt = spark.table(self.table)
        cols = tgt.columns
        types_ = dict(tgt.dtypes)
        t = tgt.withColumn("__t", F.lit(True)).alias(self.ta)
        s = self.src.withColumn("__s", F.lit(True)).alias(self.sa)
        j = t.join(s, _expr(self.cond), "full_outer")
        is_m = F.col(f"{self.ta}.__t").isNotNull() & F.col(f"{self.sa}.__s").isNotNull()
        only_t = F.col(f"{self.ta}.__t").isNotNull() & F.col(f"{self.sa}.__s").isNull()
        only_s = F.col(f"{self.ta}.__t").isNull()

        def clause_value(col, clauses, default):
            out = None
            for cond, sets in clauses:
                c = _expr(cond) if cond else F.lit(True)
                if sets == "*":
                    v = F.col(f"{self.sa}.`{col}`")
                elif col in sets:
                    v = _expr(sets[col])
                else:
                    v = default
                out = F.when(c, v) if out is None else out.when(c, v)
            return default if out is None else out.otherwise(default)

        def fired(clauses):
            conds = [F.coalesce(_expr(c), F.lit(False)) if c else F.lit(True) for c, _ in clauses]
            return reduce(lambda a, b: a | b, conds, F.lit(False))

        sel = []
        for c in cols:
            tcol = F.col(f"{self.ta}.`{c}`")
            m_val = clause_value(c, self.matched, tcol)
            nmbs_val = clause_value(c, self.nmbs, tcol)
            ins_val = clause_value(c, [(cond, v) for cond, v in self.not_matched], F.lit(None))
            v = F.when(is_m, m_val).when(only_t, nmbs_val).otherwise(ins_val)
            sel.append(v.cast(types_[c]).alias(c))
        keep = is_m | only_t | (only_s & F.coalesce(fired(self.not_matched), F.lit(False)))
        flags = [F.when(is_m & F.coalesce(fired(self.matched), F.lit(False)), 1).otherwise(0).alias("__upd"),
                 F.when(only_t & F.coalesce(fired(self.nmbs), F.lit(False)), 1).otherwise(0).alias("__nmbs"),
                 F.when(only_s, 1).otherwise(0).alias("__ins")]
        out = j.filter(keep).select(*sel, *flags).localCheckpoint(eager=True)
        # Delta rule: a target row may match at most one source row
        dup = (j.filter(is_m).groupBy(*[F.col(f"{self.ta}.`{c}`") for c in cols]).count().filter("count > 1").count())
        if dup:
            raise RuntimeError("DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE")
        m = out.agg(F.sum("__upd"), F.sum("__nmbs"), F.sum("__ins")).collect()[0]
        out.drop("__upd", "__nmbs", "__ins").write.mode("overwrite").format("parquet").saveAsTable(self.table)
        _METRICS[self.table] = {"numTargetRowsUpdated": int(m[0] or 0) + int(m[1] or 0),
                                "numTargetRowsNotMatchedBySourceUpdated": int(m[1] or 0),
                                "numTargetRowsInserted": int(m[2] or 0)}


class DeltaTable:
    def __init__(self, spark, name, alias=None):
        self.spark, self.name, self._alias = spark, name, alias

    @classmethod
    def forName(cls, spark, name):
        return cls(spark, name)

    def alias(self, a):
        return DeltaTable(self.spark, self.name, a)

    def toDF(self):
        return self.spark.table(self.name)

    def merge(self, source, condition):
        return _Merge(self.name, self._alias or "t", source, _alias_of(source), condition)

    def history(self, n=1):
        row = {"operationMetrics": {k: str(v) for k, v in _METRICS.get(self.name, {}).items()}}
        return types.SimpleNamespace(collect=lambda: [row])


def _alias_of(df):
    # DataFrame.alias("s") stores the alias in the plan: SubqueryAlias s
    plan = df._jdf.queryExecution().analyzed().toString()
    first = plan.splitlines()[0]
    return first.split("SubqueryAlias ")[1].strip() if "SubqueryAlias" in first else "s"


def install(spark: SparkSession):
    # 1. fake "delta" python package
    mod = types.ModuleType("delta"); tables = types.ModuleType("delta.tables")
    tables.DeltaTable = DeltaTable; mod.tables = tables
    sys.modules["delta"], sys.modules["delta.tables"] = mod, tables
    # 2. format("delta") -> parquet, for batch and streaming writers
    for cls in (DataFrameWriter, DataStreamWriter):
        orig = cls.format
        def fmt(self, source, _orig=orig):
            return _orig(self, "parquet" if source == "delta" else source)
        cls.format = fmt
    # 3. OPTIMIZE is a no-op
    orig_sql = spark.sql
    def sql(q, *a, **k):
        if q.strip().upper().startswith("OPTIMIZE"):
            return orig_sql("SELECT 1 AS optimized")
        return orig_sql(q, *a, **k)
    spark.sql = sql
