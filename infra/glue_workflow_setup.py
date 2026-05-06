"""
Glue Workflow setup for the cold-path pipeline.

Idempotent boto3 script that creates / updates:
  - SNS topic + email subscription for failure alerts
  - EventBridge rule that fires on FAILED/TIMEOUT/STOPPED for our cold-path
    Glue jobs and routes to SNS
  - Glue Workflow with 3 stages:
        Stage 1 (schedule trigger, DISABLED) -> 3 bronze-to-silver jobs in parallel
        Stage 2 (conditional, all 3 above SUCCEEDED) -> 3 dim jobs in parallel
        Stage 3 (conditional, all 3 dim jobs SUCCEEDED) -> facts job

Pre-reqs:
  - All 7 Glue jobs already exist in the AWS Console (Phase 4-5)
  - AWS credentials configured for region ap-southeast-1 with permission to
    create SNS topics, EventBridge rules, and Glue workflows/triggers

Usage:
    python -m infra.glue_workflow_setup
    python -m infra.glue_workflow_setup --alert-email someone@example.com

After running, check your inbox and click the SNS subscription confirmation
link, otherwise alert emails will be silently dropped.

Teardown (run from a shell):
    aws glue delete-workflow --name dw-bigdata-cold-path --region ap-southeast-1
    aws events remove-targets --rule dw-bigdata-glue-failure-alerts --ids sns-target --region ap-southeast-1
    aws events delete-rule --name dw-bigdata-glue-failure-alerts --region ap-southeast-1
    aws sns delete-topic --topic-arn <arn-from-this-script-output> --region ap-southeast-1
"""

import argparse
import json
import logging
import sys
import time
from typing import Any, Dict, List

import boto3
from botocore.exceptions import ClientError

REGION = "ap-southeast-1"
WORKFLOW_NAME = "dw-bigdata-cold-path"
SNS_TOPIC_NAME = "dw-bigdata-pipeline-alerts"
EVENTBRIDGE_RULE_NAME = "dw-bigdata-glue-failure-alerts"
DEFAULT_ALERT_EMAIL = "mond.phakapol@gmail.com"

BRONZE_TO_SILVER_JOBS: List[str] = [
    "bronze-to-silver-air",
    "bronze-to-silver-weather",
    "bronze-to-silver-sensor",
]
DIM_JOBS: List[str] = [
    "silver-to-gold-dims-scd1",
    "silver-to-gold-dim-station-scd2",
    "silver-to-gold-dim-sensor-scd2",
]
FACT_JOB: str = "silver-to-gold-facts"
ALL_JOBS: List[str] = BRONZE_TO_SILVER_JOBS + DIM_JOBS + [FACT_JOB]

# Every 6 hours at minute 0. AWS cron format: min hr dom mon dow yr
SCHEDULE_CRON = "cron(0 */6 * * ? *)"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("glue_workflow_setup")


def ensure_sns_topic(sns: Any, name: str) -> str:
    # CreateTopic is idempotent — returns the existing ARN if the topic exists.
    arn: str = sns.create_topic(Name=name)["TopicArn"]
    log.info("SNS topic ready: %s", arn)
    return arn


def ensure_email_subscription(sns: Any, topic_arn: str, email: str) -> None:
    existing = sns.list_subscriptions_by_topic(TopicArn=topic_arn)["Subscriptions"]
    for sub in existing:
        if sub["Protocol"] == "email" and sub["Endpoint"] == email:
            log.info(
                "Email subscription already present for %s (status=%s)",
                email,
                sub["SubscriptionArn"],
            )
            return
    sns.subscribe(TopicArn=topic_arn, Protocol="email", Endpoint=email)
    log.warning(
        "Subscribed %s to topic — CHECK YOUR INBOX and click the SNS confirmation link.",
        email,
    )


def set_sns_policy_for_eventbridge(sns: Any, topic_arn: str, account_id: str) -> None:
    # SNS topic policies only accept the scoped action list (SNS:Publish, SNS:Subscribe,
    # etc.) — wildcard "sns:*" is rejected with "action out of service scope".
    # Cleanest path: fetch the AWS-managed default policy that ships with new topics
    # (it grants the account owner the right scoped actions) and append our statement.
    attrs = sns.get_topic_attributes(TopicArn=topic_arn)["Attributes"]
    existing: Dict[str, Any] = json.loads(attrs.get("Policy", '{"Version":"2012-10-17","Statement":[]}'))
    statements: List[Dict[str, Any]] = [
        s for s in existing.get("Statement", []) if s.get("Sid") != "AllowEventBridgePublish"
    ]
    statements.append(
        {
            "Sid": "AllowEventBridgePublish",
            "Effect": "Allow",
            "Principal": {"Service": "events.amazonaws.com"},
            "Action": "SNS:Publish",
            "Resource": topic_arn,
        }
    )
    new_policy: Dict[str, Any] = {
        "Version": existing.get("Version", "2012-10-17"),
        "Id": existing.get("Id", "__default_policy_ID"),
        "Statement": statements,
    }
    sns.set_topic_attributes(
        TopicArn=topic_arn,
        AttributeName="Policy",
        AttributeValue=json.dumps(new_policy),
    )
    log.info("SNS topic policy updated to allow EventBridge publish.")
    _ = account_id  # unused now; kept in signature so call sites don't change


def ensure_eventbridge_rule(events: Any, sns_topic_arn: str) -> None:
    pattern: Dict[str, Any] = {
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {
            "state": ["FAILED", "TIMEOUT", "STOPPED"],
            "jobName": ALL_JOBS,
        },
    }
    events.put_rule(
        Name=EVENTBRIDGE_RULE_NAME,
        EventPattern=json.dumps(pattern),
        State="ENABLED",
        Description="Route Glue job failures (cold-path) to SNS for email alerting.",
    )
    events.put_targets(
        Rule=EVENTBRIDGE_RULE_NAME,
        Targets=[{"Id": "sns-target", "Arn": sns_topic_arn}],
    )
    log.info("EventBridge rule %s ready.", EVENTBRIDGE_RULE_NAME)


def ensure_workflow(glue: Any, name: str) -> None:
    try:
        glue.create_workflow(
            Name=name,
            Description="Cold-path pipeline: Bronze->Silver->Gold (dims then facts).",
        )
        log.info("Created workflow: %s", name)
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            log.info("Workflow %s already exists — keeping it.", name)
        else:
            raise


def recreate_trigger(glue: Any, **kwargs: Any) -> None:
    # update_trigger cannot change Type or WorkflowName, so we delete and recreate
    # to keep this script declarative — running it twice always lands the same shape.
    name: str = kwargs["Name"]
    try:
        glue.delete_trigger(Name=name)
        log.info("Deleted existing trigger: %s (waiting for delete to propagate)", name)
        for _ in range(30):
            try:
                glue.get_trigger(Name=name)
                time.sleep(1)
            except ClientError as e:
                if e.response["Error"]["Code"] == "EntityNotFoundException":
                    break
                raise
        else:
            log.warning("Trigger %s still visible after 30s — proceeding anyway.", name)
    except ClientError as e:
        if e.response["Error"]["Code"] != "EntityNotFoundException":
            raise
    glue.create_trigger(**kwargs)
    log.info("Created trigger: %s", name)


def build_triggers(glue: Any) -> None:
    # Stage 1: schedule -> bronze-to-silver. Created DISABLED (StartOnCreation=False)
    # per cost note in CLAUDE.md §7. Enable manually before video-recording week.
    recreate_trigger(
        glue,
        Name=f"{WORKFLOW_NAME}-schedule",
        Type="SCHEDULED",
        Schedule=SCHEDULE_CRON,
        Actions=[{"JobName": j} for j in BRONZE_TO_SILVER_JOBS],
        WorkflowName=WORKFLOW_NAME,
        StartOnCreation=False,
        Description="Every 6 hours. DISABLED on creation — enable before presentation week.",
    )

    # Stage 2: all 3 bronze-to-silver SUCCEEDED -> 3 dim jobs in parallel.
    recreate_trigger(
        glue,
        Name=f"{WORKFLOW_NAME}-after-bronze-to-silver",
        Type="CONDITIONAL",
        Predicate={
            "Logical": "AND",
            "Conditions": [
                {"LogicalOperator": "EQUALS", "JobName": j, "State": "SUCCEEDED"}
                for j in BRONZE_TO_SILVER_JOBS
            ],
        },
        Actions=[{"JobName": j} for j in DIM_JOBS],
        WorkflowName=WORKFLOW_NAME,
        StartOnCreation=True,
        Description="Fires when all 3 bronze-to-silver jobs succeed.",
    )

    # Stage 3: all 3 dim jobs SUCCEEDED -> facts job. Facts must wait for dims because
    # they look up surrogate keys from the dim tables.
    recreate_trigger(
        glue,
        Name=f"{WORKFLOW_NAME}-after-dims",
        Type="CONDITIONAL",
        Predicate={
            "Logical": "AND",
            "Conditions": [
                {"LogicalOperator": "EQUALS", "JobName": j, "State": "SUCCEEDED"}
                for j in DIM_JOBS
            ],
        },
        Actions=[{"JobName": FACT_JOB}],
        WorkflowName=WORKFLOW_NAME,
        StartOnCreation=True,
        Description="Fires when all 3 dim jobs succeed.",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--alert-email", default=DEFAULT_ALERT_EMAIL)
    parser.add_argument("--region", default=REGION)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    session = boto3.Session(region_name=args.region)
    sns = session.client("sns")
    events = session.client("events")
    glue = session.client("glue")
    sts = session.client("sts")

    account_id: str = sts.get_caller_identity()["Account"]
    log.info("Region=%s Account=%s", args.region, account_id)

    topic_arn = ensure_sns_topic(sns, SNS_TOPIC_NAME)
    ensure_email_subscription(sns, topic_arn, args.alert_email)
    set_sns_policy_for_eventbridge(sns, topic_arn, account_id)
    ensure_eventbridge_rule(events, topic_arn)
    ensure_workflow(glue, WORKFLOW_NAME)
    build_triggers(glue)

    log.info("Done.")
    log.info(
        "Manual run:        aws glue start-workflow-run --name %s --region %s",
        WORKFLOW_NAME,
        args.region,
    )
    log.info(
        "Enable schedule:   aws glue start-trigger --name %s-schedule --region %s",
        WORKFLOW_NAME,
        args.region,
    )
    log.info(
        "Disable schedule:  aws glue stop-trigger --name %s-schedule --region %s",
        WORKFLOW_NAME,
        args.region,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
