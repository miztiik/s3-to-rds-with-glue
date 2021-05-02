from aws_cdk import aws_ec2 as _ec2
from aws_cdk import aws_glue as _glue
from aws_cdk import aws_s3_assets as _s3_assets
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class GlueJobStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        vpc,
        my_sql_db_sg,
        store_events_db_endpoint,
        sales_events_bkt,
        _glue_etl_role,
        glue_db_name: str,
        glue_table_name: str,
        tgt_db_secret,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.template_options.metadata = {"License": "Miztiik Corp."}

        # ADD Permissions to our Glue JOB Role to Access Secrets
        tgt_db_secret.grant_read(_glue_etl_role)

        # # Create GLUE JDBC Connection for RDS MySQL

        # Allow ALL PORTS within SG for GLUE Connections to connect
        # https://docs.aws.amazon.com/glue/latest/dg/connection-defining.html#connection-properties-jdbc
        # https://docs.aws.amazon.com/glue/latest/dg/setup-vpc-for-glue-access.html
        # https://docs.amazonaws.cn/en_us/glue/latest/dg/connection-defining.html

        rds_mysql_conn_props = _glue.CfnConnection.ConnectionInputProperty(
            connection_type="JDBC",
            description="Glue Connection for RDS MySQL Store Events Database",
            name="rdsMySQL57Conn",
            physical_connection_requirements=_glue.CfnConnection.PhysicalConnectionRequirementsProperty(
                security_group_id_list=[my_sql_db_sg.security_group_id],
                subnet_id=vpc.select_subnets(
                        subnet_type=_ec2.SubnetType.PRIVATE
                ).subnet_ids[1]
            ),
            connection_properties={
                "JDBC_CONNECTION_URL": f"jdbc:mysql://{store_events_db_endpoint}:3306/store_events",
                "JDBC_ENFORCE_SSL": "false",
                "USERNAME": "mystiquemaster",
                "PASSWORD": "DUMMY_PASSWORD"
            }
        )

        rds_mysql_conn = _glue.CfnConnection(
            self,
            "rdsMySQLGlueConnection",
            catalog_id=f"{cdk.Aws.ACCOUNT_ID}",
            connection_input=rds_mysql_conn_props
        )

        # Create the Glue job to convert incoming JSON to parquet
        # Read GlueSpark Code
        try:
            with open(
                "stacks/back_end/glue_stacks/glue_job_scripts/load_json_to_rds.py",
                encoding="utf-8",
                mode="r",
            ) as f:
                load_json_to_rds = f.read()
        except OSError:
            print("Unable to read Glue Job Code")
            raise

        etl_script_asset = _s3_assets.Asset(
            self,
            "etlScriptAsset",
            path="stacks/back_end/glue_stacks/glue_job_scripts/load_json_to_rds.py"
        )

        self.etl_prefix = "stream-etl"
        _glue_etl_job = _glue.CfnJob(
            self,
            "glues3ToRdsIngestorJob",
            name="s3-to-rds-ingestor",
            description="Glue Job to ingest JSON data from S3 to RDS",
            role=_glue_etl_role.role_arn,
            glue_version="2.0",
            command=_glue.CfnJob.JobCommandProperty(
                name="glueetl",
                script_location=f"s3://{etl_script_asset.s3_bucket_name}/{etl_script_asset.s3_object_key}",
                python_version="3"
            ),
            connections={"connections": [rds_mysql_conn_props.name]},
            default_arguments={
                "--enable-metrics": True,
                "--enable-continuous-cloudwatch-log": True,
                "--job-bookmark-option": "job-bookmark-enable",
                '--TempDir': f"s3://{sales_events_bkt.bucket_name}/bookmarks",
                "--src_db_name": glue_db_name,
                "--src_etl_bkt": f"{sales_events_bkt.bucket_name}",
                "--crawler_tbl_prefix": "txns_",
                "--tgt_db_secret_arn": tgt_db_secret.secret_arn,
                "--tgt_tbl_name": glue_table_name,
                "--conn_name": f"{rds_mysql_conn_props.name}"
            },
            allocated_capacity=1,
            # timeout=2,
            max_retries=2,
            execution_property=_glue.CfnJob.ExecutionPropertyProperty(
                max_concurrent_runs=2)
        )

        # Configure a Trigger - Every hour
        _glue_etl_job_trigger = _glue.CfnTrigger(
            self,
            "glueEtlJobtrigger",
            type="SCHEDULED",
            description="Miztiik Automation: Trigger S3 to RDS Ingestor glue job every hour",
            schedule="cron(0 1 * * ? *)",
            start_on_creation=False,
            actions=[
                _glue.CfnTrigger.ActionProperty(
                    job_name=f"{_glue_etl_job.name}",
                    timeout=2
                )
            ]
        )
        _glue_etl_job_trigger.add_depends_on(_glue_etl_job)

        # Configure Glue Workflow
        _glue_etl_job_workflow = _glue.CfnWorkflow(
            self,
            "glueEtlJobWorkflow"
        )

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = cdk.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page.",
        )

        output_1 = cdk.CfnOutput(
            self,
            "RDSIngestorETLGlueJob",
            value=f"https://console.aws.amazon.com/gluestudio/home?region={cdk.Aws.REGION}#/jobs",
            description="Glue Job to ingest JSON data from S3 to RDS.",
        )
