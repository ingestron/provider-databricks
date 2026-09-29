"""Minimal stand-in for the PySpark calls made by the generated quality evaluator.

Row predicates are SQL strings; tests supply the failing-row count for each
expression, so this checks counting, comparison and failure logic only.
Native Spark SQL semantics are not exercised.
"""
import json
import sys
import types


class Expr:
    def __init__(self, kind, value=None):
        self.kind, self.value = kind, value

    def __invert__(self):
        return Expr('not', self)

    def isNotNull(self):
        return Expr('notnull', self)


class Frame:
    def __init__(self, rows, failing):
        self.rows, self.failing = rows, failing

    def where(self, condition):
        if condition.kind == 'not':  # ~coalesce(expr(sql), false)
            sql = condition.value.value[0].value
            return Frame(self.rows[:self.failing[sql]], self.failing)
        column = condition.value.value
        return Frame([r for r in self.rows if r.get(column) is not None], self.failing)

    def select(self, *columns):
        return Frame([{c: r.get(c) for c in columns} for r in self.rows], self.failing)

    def distinct(self):
        unique = {json.dumps(r, sort_keys=True) for r in self.rows}
        return Frame([json.loads(u) for u in unique], self.failing)

    def count(self):
        return len(self.rows)


functions = types.SimpleNamespace(
    expr=lambda sql: Expr('expr', sql),
    col=lambda name: Expr('col', name),
    lit=lambda value: Expr('lit', value),
    coalesce=lambda *values: Expr('coalesce', values),
)
pipelines = types.SimpleNamespace(
    create_streaming_table=lambda **_: None,
    create_auto_cdc_from_snapshot_flow=lambda **_: None,
    materialized_view=lambda **_: (lambda f: f),
)
pyspark = types.ModuleType('pyspark')
pyspark.pipelines = pipelines
sql = types.ModuleType('pyspark.sql')
sql.functions = functions
sys.modules.update({'pyspark': pyspark, 'pyspark.sql': sql,
                    'pyspark.sql.functions': functions})


def run(source, function, rows, failing, row_count):
    namespace = {}
    exec(compile(source, 'notebook.py', 'exec'), namespace)
    return namespace[function](Frame(rows, failing), row_count)
