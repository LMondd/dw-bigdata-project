# Glue Workflow — cold-path orchestration

## What this is

A single AWS Glue Workflow named **`dw-bigdata-cold-path`** that chains all
seven cold-path jobs into a 3-stage DAG, plus an EventBridge → SNS → email
alerting path for failures.

The workflow is defined by `infra/glue_workflow_setup.py` (boto3, idempotent).
Run the script once and the workflow + triggers + alerts are reproducible from
code — no console clicks needed.

## DAG

```
                    ┌─────────────────────────────────┐
                    │  schedule trigger (DISABLED)    │
                    │  cron(0 */6 * * ? *)            │
                    └────────────────┬────────────────┘
                                     │ starts in parallel
       ┌─────────────────────────────┼─────────────────────────────┐
       ▼                             ▼                             ▼
 bronze-to-silver-air     bronze-to-silver-weather    bronze-to-silver-sensor
       │                             │                             │
       └─────────────────────────────┼─────────────────────────────┘
                                     │ AND, all SUCCEEDED
                                     ▼
                    ┌─────────────────────────────────┐
                    │  conditional trigger             │
                    │  after-bronze-to-silver          │
                    └────────────────┬────────────────┘
       ┌─────────────────────────────┼─────────────────────────────┐
       ▼                             ▼                             ▼
 silver-to-gold-          silver-to-gold-              silver-to-gold-
   dims-scd1               dim-station-scd2             dim-sensor-scd2
       │                             │                             │
       └─────────────────────────────┼─────────────────────────────┘
                                     │ AND, all SUCCEEDED
                                     ▼
                    ┌─────────────────────────────────┐
                    │  conditional trigger             │
                    │  after-dims                      │
                    └────────────────┬────────────────┘
                                     ▼
                            silver-to-gold-facts
```

### Why this shape

- **Stage 1 jobs are independent** — different Bronze sources, no shared state.
  Run them in parallel to compress wall-clock.
- **Stage 2 dim jobs are independent** of each other but must finish before
  facts: facts join on dim surrogate keys.
- **Stage 3 (facts) is one job** by design — see `glue_jobs/silver_to_gold_facts.py`.
- The conditional triggers use `Logical: AND` so any single-job failure halts
  downstream, which is what we want — incomplete dims would corrupt facts.

## Failure alerting

- **EventBridge rule** `dw-bigdata-glue-failure-alerts` matches
  `Glue Job State Change` events with `state in ['FAILED','TIMEOUT','STOPPED']`
  and `jobName in <our 7 jobs>`.
- Target is the SNS topic `dw-bigdata-pipeline-alerts`.
- Email `mond.phakapol@gmail.com` is subscribed.
- **You must click the SNS confirmation email after first run**, otherwise
  every alert is silently dropped. Status appears as `PendingConfirmation` in
  the SNS console until you click.

## Running the setup

```bash
cd ~/DW/dw-bigdata-project
python -m infra.glue_workflow_setup
```

Idempotent: re-running drops and recreates the triggers (cleanest way to apply
schema changes), and SNS / EventBridge / workflow operations are upserts.

## Operating

```bash
# Run the whole DAG once, on demand:
aws glue start-workflow-run --name dw-bigdata-cold-path --region ap-southeast-1

# Enable the 6-hourly schedule (do this only for video week / live demo):
aws glue start-trigger --name dw-bigdata-cold-path-schedule --region ap-southeast-1

# Disable it again to stop the bill:
aws glue stop-trigger --name dw-bigdata-cold-path-schedule --region ap-southeast-1

# Check status of the most recent run:
aws glue get-workflow-runs --name dw-bigdata-cold-path --max-results 1 --region ap-southeast-1
```

## Cost expectations (per CLAUDE.md §7)

- Glue Workflow itself, EventBridge rule, SNS topic+email: effectively free.
- Per workflow run = 7 Glue job runs ≈ **~$0.30** at G.1X / ~5 min each.
- Schedule enabled (every 6 hr) = ~$1.20 / day = **~$36 / month**.
- **Recommendation:** keep schedule DISABLED (the default this script ships
  with). Trigger manually for testing and demos. Enable only during the week
  of video recording so the dashboards look fresh.

Update `AWS_Cost_Tracker.xlsx` after each run while iterating.

## Screenshot checklist (for the DE video, rubric §Automatic Workflow)

Capture from the AWS Console under Glue → ETL jobs → Workflows → `dw-bigdata-cold-path`:

- [ ] Workflow graph view (the DAG renders nicely — main money shot)
- [ ] Triggers tab (3 triggers visible, schedule shown as DISABLED)
- [ ] One successful workflow run, expanded to show all 7 nodes green
- [ ] EventBridge rules → `dw-bigdata-glue-failure-alerts` showing pattern + SNS target
- [ ] SNS topic → confirmed email subscription
- [ ] Optional: a deliberately-failed run (e.g., wrong --bucket_name) producing an alert email — strong evidence the alert path is wired

## Teardown

```bash
aws glue delete-workflow --name dw-bigdata-cold-path --region ap-southeast-1
aws events remove-targets --rule dw-bigdata-glue-failure-alerts --ids sns-target --region ap-southeast-1
aws events delete-rule --name dw-bigdata-glue-failure-alerts --region ap-southeast-1
# Replace ARN with the one logged by the setup script:
aws sns delete-topic --topic-arn arn:aws:sns:ap-southeast-1:<account-id>:dw-bigdata-pipeline-alerts --region ap-southeast-1
```

The 7 underlying Glue jobs are NOT touched by this teardown — they were
created in Phases 4-5 and are managed separately.
