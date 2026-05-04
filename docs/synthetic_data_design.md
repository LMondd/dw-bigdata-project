# Synthetic IoT Sensor Data Design

## Why Synthetic?
Real sub-second IoT data is hard to source legally.
This generator provides the Velocity + Volume story for the Big Data rubric.
Backfill mode generates 30 days of history in minutes (~1M+ rows).

## Sensor Scenario
A hypothetical industrial facility in Bangkok with 5 zones and 50 sensors.

## Zones
| Zone ID | Zone Name | Description |
|---|---|---|
| Z01 | Production A | Main manufacturing floor |
| Z02 | Production B | Secondary manufacturing floor |
| Z03 | Warehouse | Storage area |
| Z04 | Utilities | Power and cooling systems |
| Z05 | Office | Administrative area |

## Sensors (50 total)
| Type | Count | Zone distribution | Target value | Anomaly rate |
|---|---|---|---|---|
| temperature | 20 | 4 per zone | 22°C | 1% |
| pressure | 15 | 3 per zone | 100 kPa | 2% |
| vibration | 10 | 2 per zone | 0.5 mm/s | 1.5% |
| flow | 5 | 1 per zone | 50 L/min | 1% |

## Reading Cadence
- Live mode: 1 reading per sensor per 5 seconds → ~864k rows/day
- Backfill mode: generates 30 days instantly → ~26M rows target

## Realistic Patterns
- Daily cycle: temperature peaks at 14:00, lowest at 04:00
- Weekly cycle: lower readings on weekends
- Drift: vibration sensors drift upward over hours
- Anomalies: random spikes beyond 3-sigma threshold

## Kafka Topic
- Topic: sensor-stream
- Partitions: 6
- Key: sensor_id
