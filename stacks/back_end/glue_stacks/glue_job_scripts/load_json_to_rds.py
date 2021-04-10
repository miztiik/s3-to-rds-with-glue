import sys
import logging
import boto3
import json
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "src_db_name",
    "src_etl_bkt",
    "crawler_tbl_prefix",
    "tgt_db_secret_arn",
    "tgt_tbl_name",
    "conn_name"
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Get RDS DB credentials from Secrets Manager
client = boto3.client("secretsmanager")
get_rds_secret = client.get_secret_value(SecretId=args["tgt_db_secret_arn"])
secret = get_rds_secret["SecretString"]
secret = json.loads(secret)

tgt_db_username = secret.get("username")
tgt_db_password = secret.get("password")
tgt_db_endpoint = secret.get("host")
tgt_db_name = secret.get("dbname")
tgt_db_port = secret.get("port")

# Glue Crawler creates tables with underscores
glue_src_tbl_name = f'{args["crawler_tbl_prefix"]}{args["src_etl_bkt"].replace("-", "_")}'

logger.info(f'{{"starting_job": "{args["JOB_NAME"]}"}}')

# Construct JDBC connection options
connection_mysql5_options = {
    "url": f"jdbc:mysql://{tgt_db_endpoint}:{tgt_db_port}/{tgt_db_name}",
    "dbtable": glue_src_tbl_name,
    "database": f"{tgt_db_name}",
    "user": f"{tgt_db_username}",
    "password": f"{tgt_db_password}"
}

logger.info(f'{{"conn_properties": {connection_mysql5_options}}}')

# connection_mysql8_options = {
#     "url": f"jdbc:mysql://{}:3306/db",
#     "dbtable": "test",
#     "user": "admin",
#     "password": "pwd",
#     "customJdbcDriverS3Path": "s3://path/mysql-connector-java-8.0.17.jar",
#     "customJdbcDriverClassName": "com.mysql.cj.jdbc.Driver"}


datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database=args["src_db_name"],
    table_name=glue_src_tbl_name,
    transformation_ctx="datasource0"
)

applymapping1 = ApplyMapping.apply(
    frame=datasource0, mappings=[
        ("request_id", "string", "request_id", "string"),
        ("category", "string", "category", "string"),
        ("store_id", "int", "store_id", "int"),
        ("ts", "string", "ts", "string"),
        ("event_type", "string", "event_type", "string"),
        ("sales", "double", "sales", "double"),
        ("sku", "int", "sku", "int"),
        ("gift_wrap", "boolean", "gift_wrap", "boolean"),
        ("qty", "int", "qty", "int"),
        ("priority_shipping", "boolean", "priority_shipping", "boolean"),
        ("contact_me", "string", "contact_me", "string"),
        ("is_return", "boolean", "is_return", "boolean"),
        ("bad_msg", "boolean", "bad_msg", "boolean"),
        ("partition_event_type", "string", "partition_event_type", "string"),
        ("dt", "string", "dt", "string")
    ],
    transformation_ctx="applymapping1"
)

resolvechoice2 = ResolveChoice.apply(
    frame=applymapping1, choice="make_cols", transformation_ctx="resolvechoice2")

dropnullfields3 = DropNullFields.apply(
    frame=resolvechoice2, transformation_ctx="dropnullfields3")

# datasink4 = glueContext.write_from_options(
#     frame_or_dfc=dropnullfields3,
#     connection_type="mysql",
#     connection_options=connection_mysql5_options,
#     transformation_ctx="datasink4"
# )


datasink4 = glueContext.write_dynamic_frame.from_jdbc_conf(
    frame=dropnullfields3,
    catalog_connection=args["conn_name"],
    connection_options={"dbtable": args["tgt_tbl_name"], "database": tgt_db_name,
                        "user": tgt_db_username, "password": tgt_db_password},
    transformation_ctx="datasink4"
)

job.commit()
