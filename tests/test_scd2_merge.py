"""Tests for SCD2 merge logic. The hardest piece — test it well."""

# TODO: pyspark testing setup, then cases:
#   - no change → no new row
#   - attribute change → close old, insert new
#   - new natural key → insert with is_current=True
#   - deleted natural key → handling decision (soft-delete? leave as-is?)
