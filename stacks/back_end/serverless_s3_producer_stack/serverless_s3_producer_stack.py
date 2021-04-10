from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_iam as _iam
from aws_cdk import aws_logs as _logs
from aws_cdk import core as cdk
from stacks.miztiik_global_args import GlobalArgs


class ServerlessS3ProducerStack(cdk.Stack):
    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        sales_event_bkt,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add your stack resources below):

        ########################################
        #######                          #######
        #######   Stream Data Producer   #######
        #######                          #######
        ########################################

        # Read Lambda Code
        try:
            with open(
                "stacks/back_end/serverless_s3_producer_stack/lambda_src/stream_data_producer.py",
                encoding="utf-8",
                mode="r",
            ) as f:
                data_producer_fn_code = f.read()
        except OSError:
            print("Unable to read Lambda Function Code")
            raise

        data_producer_fn = _lambda.Function(
            self,
            "streamDataProducerFn",
            function_name=f"data_producer_{construct_id}",
            description="Produce streaming data events and push to S3 stream",
            runtime=_lambda.Runtime.PYTHON_3_7,
            code=_lambda.InlineCode(data_producer_fn_code),
            handler="index.lambda_handler",
            timeout=cdk.Duration.seconds(2),
            reserved_concurrent_executions=1,
            environment={
                "LOG_LEVEL": "INFO",
                "APP_ENV": "Production",
                "STORE_EVENTS_BKT": f"{sales_event_bkt.bucket_name}",
                "TRIGGER_RANDOM_DELAY": "True"
            },
        )

        # Grant our Lambda Producer privileges to write to S3
        sales_event_bkt.grant_read_write(data_producer_fn)

        data_producer_fn_version = data_producer_fn.latest_version
        data_producer_fn_version_alias = _lambda.Alias(
            self,
            "streamDataProducerFnAlias",
            alias_name="MystiqueAutomation",
            version=data_producer_fn_version,
        )

        # Create Custom Loggroup for Producer
        data_producer_lg = _logs.LogGroup(
            self,
            "streamDataProducerFnLogGroup",
            log_group_name=f"/aws/lambda/{data_producer_fn.function_name}",
            removal_policy=cdk.RemovalPolicy.DESTROY,
            retention=_logs.RetentionDays.ONE_DAY,
        )

        # Restrict Produce Lambda to be invoked only from the stack owner account
        data_producer_fn.add_permission(
            "restrictLambdaInvocationToOwnAccount",
            principal=_iam.AccountRootPrincipal(),
            action="lambda:InvokeFunction",
            source_account=cdk.Aws.ACCOUNT_ID,
            # source_arn=sales_event_bkt.bucket_arn
        )

        self.data_producer_fn_role = data_producer_fn.role

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
            "LambdaConsumer",
            # "StoreOrdersEventsProducer",
            value=f"https://console.aws.amazon.com/lambda/home?region={cdk.Aws.REGION}#/functions/{data_producer_fn.function_name}",
            description="Produce streaming data events and push to S3 Topic.",
        )
        output_2 = cdk.CfnOutput(
            self,
            "LambdaConsumerRoleArn",
            value=f"{self.data_producer_fn_role.role_arn}",
            description="StoreOrdersEventsProducerRole",
        )
