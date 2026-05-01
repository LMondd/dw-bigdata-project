"""Reusable SCD2 merge logic for PySpark/Glue.

TODO: implement the standard SCD2 pattern:
  - Inputs: current dim DF + incoming source DF (one row per natural key)
  - Compare incoming attributes vs current `is_current=True` rows
  - For changed rows: set old row's end_date and is_current=False, insert new row
  - For new natural keys: insert as type-2 records with is_current=True
  - Generate surrogate keys (monotonically_increasing_id() or hash-based)
"""
