from aws_cdk import aws_ec2 as _ec2
from aws_cdk import aws_iam as _iam
from aws_cdk import core as cdk


from stacks.miztiik_global_args import GlobalArgs


class OltpConsumerOnEC2Stack(cdk.Stack):

    def __init__(
        self,
        scope: cdk.Construct,
        construct_id: str,
        stack_log_level: str,
        vpc,
        ec2_instance_type: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read BootStrap Script):
        try:
            with open("stacks/back_end/oltp_consumer_on_ec2_stack/bootstrap_scripts/deploy_app.sh",
                      encoding="utf-8",
                      mode="r"
                      ) as f:
                user_data = f.read()
        except OSError as e:
            print("Unable to read UserData script")
            raise e

        # Get the latest AMI from AWS SSM
        linux_ami = _ec2.AmazonLinuxImage(
            generation=_ec2.AmazonLinuxGeneration.AMAZON_LINUX_2)

        # Get the latest ami
        amzn_linux_ami = _ec2.MachineImage.latest_amazon_linux(
            generation=_ec2.AmazonLinuxGeneration.AMAZON_LINUX_2
        )
        # ec2 Instance Role
        self._instance_role = _iam.Role(
            self, "webAppClientRole",
            assumed_by=_iam.ServicePrincipal(
                "ec2.amazonaws.com"),
            managed_policies=[
                _iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonSSMManagedInstanceCore"
                )
            ]
        )

        # Allow CW Agent to create Logs
        self._instance_role.add_to_policy(_iam.PolicyStatement(
            actions=[
                "logs:Create*",
                "logs:PutLogEvents"
            ],
            resources=["arn:aws:logs:*:*:*"]
        ))

        # app_server Instance
        app_server = _ec2.Instance(
            self,
            "appServer",
            instance_type=_ec2.InstanceType(
                instance_type_identifier=f"{ec2_instance_type}"),
            instance_name="rds_consumer_01",
            machine_image=amzn_linux_ami,
            vpc=vpc,
            vpc_subnets=_ec2.SubnetSelection(
                subnet_type=_ec2.SubnetType.PUBLIC
            ),
            role=self._instance_role,
            user_data=_ec2.UserData.custom(
                user_data)
        )

        # Allow Web Traffic to WebServer
        app_server.connections.allow_from_any_ipv4(
            _ec2.Port.tcp(80),
            description="Allow Incoming HTTP Traffic"
        )

        # app_server.connections.allow_internally(
        #     port_range=_ec2.Port.tcp(3306),
        #     description="Allow Incoming MySQL Traffic"
        # )

        app_server.connections.allow_from(
            other=_ec2.Peer.ipv4(vpc.vpc_cidr_block),
            port_range=_ec2.Port.tcp(80),
            description="Allow Incoming Web Traffic"
        )

        # Allow CW Agent to create Logs
        self._instance_role.add_to_policy(_iam.PolicyStatement(
            actions=[
                "logs:Create*",
                "logs:PutLogEvents"
            ],
            resources=["arn:aws:logs:*:*:*"]
        ))

        ###########################################
        ################# OUTPUTS #################
        ###########################################
        output_0 = cdk.CfnOutput(
            self,
            "AutomationFrom",
            value=f"{GlobalArgs.SOURCE_INFO}",
            description="To know more about this automation stack, check out our github page."
        )
        output_1 = cdk.CfnOutput(
            self,
            "ConsumerPrivateIp",
            value=f"http://{app_server.instance_private_ip}",
            description=f"Private IP of App on EC2"
        )
        output_2 = cdk.CfnOutput(
            self,
            "Ec2ConsumerInstance",
            value=(
                f"https://console.aws.amazon.com/ec2/v2/home?region="
                f"{cdk.Aws.REGION}"
                f"#Instances:search="
                f"{app_server.instance_id}"
                f";sort=instanceId"
            ),
            description=f"Login to the instance using Systems Manager and use curl to access Urls"
        )
