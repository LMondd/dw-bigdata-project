# Dimensional Model

> TODO: full star schema, bus matrix, SCD decisions.
> See `CLAUDE.md` Section 4 for the summary.

## Business processes

1. Hourly air quality monitoring (Air4Thai)
2. Daily pollution summary (derived)
3. High-frequency sensor monitoring (synthetic IoT)
4. Sensor anomaly events (hot-path output)

## Bus matrix

| Process | dim_date | dim_station | dim_sensor | dim_zone | dim_region | dim_weather_condition |
|---|---|---|---|---|---|---|
| Hourly air quality | ✓ | ✓ |  |  | ✓ | ✓ |
| Daily pollution summary | ✓ | ✓ |  |  | ✓ |  |
| Sensor reading | ✓ |  | ✓ | ✓ |  |  |
| Sensor anomaly event | ✓ |  | ✓ | ✓ |  |  |

## Fact tables

TODO: full DDL with grain, columns, types.

## Dimension tables

TODO: full DDL, SCD type per dimension with reasoning.
