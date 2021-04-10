#!/usr/bin/env python3

from aws_cdk import core as cdk

from stacks.back_end.vpc_stack import VpcStack
from stacks.back_end.s3_stack.s3_stack import S3Stack
from stacks.back_end.rds_stack import RdsDatabaseStack
from stacks.back_end.oltp_consumer_on_ec2_stack.oltp_consumer_on_ec2_stack import OltpConsumerOnEC2Stack
from stacks.back_end.glue_stacks.glue_table_stack import GlueTableStack
from stacks.back_end.glue_stacks.glue_job_stack import GlueJobStack
from stacks.back_end.glue_stacks.glue_crawler_stack import GlueCrawlerStack

from stacks.back_end.serverless_s3_producer_stack.serverless_s3_producer_stack import ServerlessS3ProducerStack

app = cdk.App()

# S3 Bucket to hold our store events
sales_events_bkt_stack = S3Stack(
    app,
    # f"{app.node.try_get_context('project')}-sales-events-bkt-stack",
    f"sales-events-bkt-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: S3 Bucket to hold our store events"
)

# S3 Sales Event Data Producer on Lambda
sales_events_producer_stack = ServerlessS3ProducerStack(
    app,
    f"sales-events-producer-stack",
    stack_log_level="INFO",
    sales_event_bkt=sales_events_bkt_stack.data_bkt,
    description="Miztiik Automation: S3 Sales Event Data Producer on Lambda")


# Glue Stacks
glue_tbl_stack = GlueTableStack(
    app,
    f"{app.node.try_get_context('project')}-txns-tbl-stack",
    stack_log_level="INFO",
    sales_event_bkt=sales_events_bkt_stack.data_bkt,
    description="Miztiik Automation: Glue Table Stack"
)

# Glue Crawler
glue_crawler_stack = GlueCrawlerStack(
    app,
    f"{app.node.try_get_context('project')}-crawler-stack",
    stack_log_level="INFO",
    sales_event_bkt=sales_events_bkt_stack.data_bkt,
    glue_db_name=glue_tbl_stack.glue_db_name.value_as_string,
    description="Miztiik Automation: Glue Crawler Stack"
)


# VPC Stack for hosting Secure workloads & Other resources
vpc_stack = VpcStack(
    app,
    f"{app.node.try_get_context('project')}-vpc-stack",
    stack_log_level="INFO",
    description="Miztiik Automation: Custom Multi-AZ VPC"
)

# Deploy RDS Consumer EC2 instance
oltp_consumer_on_ec2_stack = OltpConsumerOnEC2Stack(
    app,
    f"oltp-consumer-on-ec2-stack",
    vpc=vpc_stack.vpc,
    ec2_instance_type="t2.micro",
    stack_log_level="INFO",
    description="Miztiik Automation: Deploy RDS Consumer EC2 instance"
)

# Sales Events Database on RDS with MySQL
sales_events_otlp_db_stack = RdsDatabaseStack(
    app,
    f"sales-events-oltp-db-stack",
    vpc=vpc_stack.vpc,
    rds_instance_size="r5.large",  # db. prefix is added by cdk automatically
    stack_log_level="INFO",
    description="Miztiik Automation: Sales Events Database on RDS with MySQL",
)


# Glue Job Stack
glue_job_stack = GlueJobStack(
    app,
    f"{app.node.try_get_context('project')}-job-stack",
    stack_log_level="INFO",
    vpc=vpc_stack.vpc,
    my_sql_db_sg=sales_events_otlp_db_stack.my_sql_db_sg,
    store_events_db_endpoint=sales_events_otlp_db_stack.store_events_db_endpoint,
    sales_events_bkt=sales_events_bkt_stack.data_bkt,
    _glue_etl_role=glue_crawler_stack._glue_etl_role,
    glue_db_name=glue_tbl_stack.glue_db_name.value_as_string,
    glue_table_name=glue_tbl_stack.glue_table_name.value_as_string,
    tgt_db_secret=sales_events_otlp_db_stack.rds_secret,
    description="Miztiik Automation: Glue Job Stack"
)


# Stack Level Tagging
_tags_lst = app.node.try_get_context("tags")

if _tags_lst:
    for _t in _tags_lst:
        for k, v in _t.items():
            cdk.Tags.of(app).add(
                k, v, apply_to_launched_instances=True, priority=300)

app.synth()
