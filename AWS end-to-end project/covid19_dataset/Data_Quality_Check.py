import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1706349608963 = glueContext.create_dynamic_frame.from_catalog(
    database="data_quality_db",
    table_name="data_quality_dataqualitycheck",
    transformation_ctx="AWSGlueDataCatalog_node1706349608963",
)

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1706349784323_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
    ColumnCount = 3,
    IsComplete "col0",
    RowCount > 15000
    ]
"""

EvaluateDataQuality_node1706349784323 = EvaluateDataQuality().process_rows(
    frame=AWSGlueDataCatalog_node1706349608963,
    ruleset=EvaluateDataQuality_node1706349784323_ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1706349784323",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": "s3://dataqualitycheck",
    },
    additional_options={"performanceTuning.caching": "CACHE_NOTHING"},
)

assert (
    EvaluateDataQuality_node1706349784323[
        EvaluateDataQuality.DATA_QUALITY_RULE_OUTCOMES_KEY
    ]
    .filter(lambda x: x["Outcome"] == "Failed")
    .count()
    == 0
), "The job failed due to failing DQ rules for node: AWSGlueDataCatalog_node1706349608963"

# Script generated for node ruleOutcomes
ruleOutcomes_node1706350327976 = SelectFromCollection.apply(
    dfc=EvaluateDataQuality_node1706349784323,
    key="ruleOutcomes",
    transformation_ctx="ruleOutcomes_node1706350327976",
)

# Script generated for node Amazon S3
AmazonS3_node1706350725218 = glueContext.write_dynamic_frame.from_options(
    frame=ruleOutcomes_node1706350327976,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://dataqualityresulttest", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1706350725218",
)

job.commit()
