from aws_cdk import aws_glue as _glue
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class GlueTableStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        sales_event_bkt,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.template_options.description = "Miztiik Automation: Sales Transactions Table Stack."
        self.template_options.metadata = {"License": "Miztiik Corp."}

        # CloudFormation Parameters

        self.glue_db_name = cdk.CfnParameter(
            self,
            "GlueTxnsDbName",
            type="String",
            description="Name of Glue Database to be created for Sales Transactions.",
            allowed_pattern="[\w-]+",
            default="miztiik_sales_db",
        )

        self.glue_table_name = cdk.CfnParameter(
            self,
            "GlueTxnsTableName",
            type="String",
            description="Name of Glue Table to be created for Sales Transactions (JSON).",
            allowed_pattern="[\w-]+",
            default="sales_txns_tbl",
        )

        cfn_txn_db = _glue.CfnDatabase(
            self,
            "GlueTxnsDb",
            catalog_id=cdk.Aws.ACCOUNT_ID,
            database_input=_glue.CfnDatabase.DatabaseInputProperty(
                name=self.glue_db_name.value_as_string,
                description="Database for Sales Transactions."
                # location_uri=txns_bucket.s3_url_for_object(),
            ),
        )

        # Ref: https://docs.aws.amazon.com/glue/latest/dg/add-job-streaming.html
        cfn_txn_table = _glue.CfnTable(
            self,
            "glueTxnsTable01",
            catalog_id=cfn_txn_db.catalog_id,
            database_name=self.glue_db_name.value_as_string,
            table_input=_glue.CfnTable.TableInputProperty(
                description="Sales Transactions Table",
                name=self.glue_table_name.value_as_string,
                parameters={
                    "classification": "json",
                    # "typeOfData": "file"
                },
                # partition_keys=[
                #     {
                #         "name": "product_category",
                #         "type": "string"
                #     }
                # ],
                table_type="EXTERNAL_TABLE",
                storage_descriptor=_glue.CfnTable.StorageDescriptorProperty(
                    # columns=[
                    #     {
                    #         "name": "marketplace",
                    #         "type": "string"
                    #     }
                    # ],
                    location=f"s3://{sales_event_bkt.bucket_name}",
                    parameters={
                        "compressionType": "none",
                        "typeOfData": "file",
                        "classification": "json"
                    },
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=_glue.CfnTable.SerdeInfoProperty(
                        name="miztiikAutomationSerDeConfig",
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                        parameters={
                            "paths": "",
                            # "typeOfData": "file"
                        }
                    )
                )
            )
        )

        cfn_txn_table.add_depends_on(cfn_txn_db)

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
            "GlueCatalogDBForStoreEvents",
            value=f"https://console.aws.amazon.com/glue/home?region={cdk.Aws.REGION}#table:catalog={cdk.Aws.ACCOUNT_ID};name={self.glue_table_name.value_as_string}",
            description="Glue Transactions Table.",
        )

        output_2 = cdk.CfnOutput(
            self,
            "GlueTxnsTable",
            value=f"https://console.aws.amazon.com/glue/home?region={cdk.Aws.REGION}#table:catalog={cdk.Aws.ACCOUNT_ID};name={self.glue_table_name.value_as_string};namespace={self.glue_db_name.value_as_string}",
            description="Glue Transactions Table.",
        )
