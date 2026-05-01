# Architecture

> TODO: paste/embed the architecture diagram here. Use draw.io or Excalidraw.

## High-level flow

```
SOURCES → KAFKA → ┬─ HOT PATH  → DynamoDB → live dashboard
                  └─ COLD PATH → S3 (Bronze→Silver→Gold) → Athena → QuickSight
```

## Component decisions

See `CLAUDE.md` Section 2 for the locked decisions table.
