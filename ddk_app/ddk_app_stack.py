from typing import Any

from aws_ddk_core.base import BaseStack
from aws_ddk_core.resources import *
from constructs import Construct

from aws_cdk import aws_glue_alpha as glue
import aws_cdk.aws_athena as athena
from aws_cdk.aws_ec2 import *
from aws_cdk import aws_dms
from aws_cdk import aws_iam
from aws_cdk import Duration


### DDK App Stack

### Source data is from TollData from this workshop: https://catalog.us-east-1.prod.workshops.aws/workshops/4ee30ea1-f102-4563-a422-8d6364ded7d2/en-US
### Iceberg sample from this: https://aws.amazon.com/blogs/big-data/implement-a-cdc-based-upsert-in-a-data-lake-using-apache-iceberg-and-aws-glue/
### List of requirements not on this DDK/CDK template manually created:
### # VPC / Subnets
### # DMS Subnet Group
### # RDS Instance (Source Database)
### # RDS Inbound allow DMS
### # AWS Secrets Manager Secret (Source Database)
### # Subscription to "Apache Iceberg Connector for AWS Glue" (AWS Marketplace)
### # Athena Scripts (Glue Catalog Tables)
### # Under Job parameters, specify Key as --iceberg_job_catalog_warehouse and Value as your S3 path (e.g. s3://<bucket-name>/<iceberg-warehouse-path>).
### # Enable Job Bookmarks for the Glue job


class DdkApplicationStack(BaseStack):
    def __init__(
        self, scope: Construct, id: str, environment_id: str, **kwargs: Any
    ) -> None:
        super().__init__(scope, id, environment_id, **kwargs)

        ddk_bucket = S3Factory.bucket(
            self,
            "lakehouse-blackbelt-bucket",
            environment_id,
            event_bridge_enabled=True,
        )

        defaultVPC = Vpc.from_lookup(
            self, "default-vpc", vpc_id="vpc-0ba1ac1b3ca7a3efb"
        )

        dms_sg = SecurityGroup(
            self,
            "dms_sg",
            vpc=defaultVPC,
            allow_all_outbound=True,
            description="Security Group for DMS",
        )

        dms_instance = DMSFactory.replication_instance(
            self,
            "lakehouse-blackbelt-dms-instance",
            environment_id,
            replication_instance_class="dms.t3.micro",
            allocated_storage=50,
            publicly_accessible=True,
            replication_subnet_group_identifier="defaultsubnetgroup",
            vpc_security_group_ids=[dms_sg.security_group_id],
        )
        dms_role = aws_iam.Role(
            self,
            "dms_role",
            assumed_by=aws_iam.ServicePrincipal("dms.us-east-1.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "SecretsManagerReadWrite"
                )
            ],
        )

        dms_source_endpoint = DMSFactory.endpoint(
            self,
            "mysql_source_endpoint",
            environment_id,
            endpoint_type="source",
            engine_name="aurora",
            s3_settings=None,
            my_sql_settings=aws_dms.CfnEndpoint.MySqlSettingsProperty(
                clean_source_metadata_on_mismatch=False,
                events_poll_interval=5,
                max_file_size=512,
                parallel_load_threads=2,
                secrets_manager_access_role_arn=dms_role.role_arn,
                secrets_manager_secret_id="blackbelt-lab-aurora-mysql-rds"
                # server_timezone="serverTimezone",
            ),
        )

        s3_target_endpoint_settings = DMSFactory.endpoint_settings_s3(
            self,
            "s3_target_endpoint_settings",
            environment_id,
            ddk_bucket.bucket_name,
            bucket_folder="lakehouse/tolldata_landing_csv",
        )

        dms_target_endpoint = DMSFactory.endpoint(
            self,
            "black_belt_s3_target_endpoint",
            environment_id,
            endpoint_type="target",
            engine_name="s3",
            s3_settings=s3_target_endpoint_settings,
        )

        dms_cdc_task_blackbelt = DMSFactory.replication_task(
            self,
            "dms_cdc_task_blackbelt",
            environment_id,
            replication_instance_arn=dms_instance.ref,
            source_endpoint_arn=dms_source_endpoint.ref,
            target_endpoint_arn=dms_target_endpoint.ref,
            table_mappings="""{
                "rules": [
                    {
                    "rule-type": "selection",
                    "rule-id": "112298883",
                    "rule-name": "112298883",
                    "object-locator": {
                        "schema-name": "TollData",
                        "table-name": "%"
                    },
                    "rule-action": "include",
                    "filters": []
                    },
                    {
                    "rule-type": "transformation",
                    "rule-id": "2",
                    "rule-name": "2",
                    "rule-action": "add-column",
                    "rule-target": "column",
                    "object-locator": {
                        "schema-name": "TollData",
                        "table-name": "%"
                    },
                    "value": "cdc_ingestion_timestamp",
                    "expression": "datetime ()",
                    "data-type": {
                        "type": "datetime",
                        "precision": 6
                    }
                    }
                ]
            }""",
            migration_type="full-load-and-cdc",
            replication_task_settings="""{
                "TargetMetadata": {
                    "TargetSchema": "",
                    "SupportLobs": true,
                    "FullLobMode": false,
                    "LobChunkSize": 64,
                    "LimitedSizeLobMode": true,
                    "LobMaxSize": 32,
                    "InlineLobMaxSize": 0,
                    "LoadMaxFileSize": 0,
                    "ParallelLoadThreads": 0,
                    "ParallelLoadBufferSize": 0,
                    "BatchApplyEnabled": false,
                    "TaskRecoveryTableEnabled": false,
                    "ParallelLoadQueuesPerThread": 0,
                    "ParallelApplyThreads": 0,
                    "ParallelApplyBufferSize": 0,
                    "ParallelApplyQueuesPerThread": 0
                },
                "FullLoadSettings": {
                    "CreatePkAfterFullLoad": false,
                    "StopTaskCachedChangesApplied": false,
                    "StopTaskCachedChangesNotApplied": false,
                    "MaxFullLoadSubTasks": 8,
                    "TransactionConsistencyTimeout": 600,
                    "CommitRate": 10000
                },
                "Logging": {
                    "EnableLogging": false,
                    "LogComponents": [
                    {
                        "Id": "SOURCE_UNLOAD",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "SOURCE_CAPTURE",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TARGET_LOAD",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TARGET_APPLY",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    },
                    {
                        "Id": "TASK_MANAGER",
                        "Severity": "LOGGER_SEVERITY_DEFAULT"
                    }
                    ],
                    "CloudWatchLogGroup": null,
                    "CloudWatchLogStream": null
                },
                "ControlTablesSettings": {
                    "ControlSchema": "",
                    "HistoryTimeslotInMinutes": 5,
                    "HistoryTableEnabled": false,
                    "SuspendedTablesTableEnabled": false,
                    "StatusTableEnabled": false
                },
                "StreamBufferSettings": {
                    "StreamBufferCount": 3,
                    "StreamBufferSizeInMB": 8,
                    "CtrlStreamBufferSizeInMB": 5
                },
                "ChangeProcessingDdlHandlingPolicy": {
                    "HandleSourceTableDropped": true,
                    "HandleSourceTableTruncated": true,
                    "HandleSourceTableAltered": true
                },
                "ErrorBehavior": {
                    "DataErrorPolicy": "LOG_ERROR",
                    "DataTruncationErrorPolicy": "LOG_ERROR",
                    "DataErrorEscalationPolicy": "SUSPEND_TABLE",
                    "DataErrorEscalationCount": 0,
                    "TableErrorPolicy": "SUSPEND_TABLE",
                    "TableErrorEscalationPolicy": "STOP_TASK",
                    "TableErrorEscalationCount": 0,
                    "RecoverableErrorCount": -1,
                    "RecoverableErrorInterval": 5,
                    "RecoverableErrorThrottling": true,
                    "RecoverableErrorThrottlingMax": 1800,
                    "RecoverableErrorStopRetryAfterThrottlingMax": false,
                    "ApplyErrorDeletePolicy": "IGNORE_RECORD",
                    "ApplyErrorInsertPolicy": "LOG_ERROR",
                    "ApplyErrorUpdatePolicy": "LOG_ERROR",
                    "ApplyErrorEscalationPolicy": "LOG_ERROR",
                    "ApplyErrorEscalationCount": 0,
                    "ApplyErrorFailOnTruncationDdl": false,
                    "FullLoadIgnoreConflicts": true,
                    "FailOnTransactionConsistencyBreached": false,
                    "FailOnNoTablesCaptured": false
                },
                "ChangeProcessingTuning": {
                    "BatchApplyPreserveTransaction": true,
                    "BatchApplyTimeoutMin": 1,
                    "BatchApplyTimeoutMax": 30,
                    "BatchApplyMemoryLimit": 500,
                    "BatchSplitSize": 0,
                    "MinTransactionSize": 1000,
                    "CommitTimeout": 1,
                    "MemoryLimitTotal": 1024,
                    "MemoryKeepTime": 60,
                    "StatementCacheSize": 50
                },
                "ValidationSettings": {
                    "EnableValidation": false,
                    "ValidationMode": "ROW_LEVEL",
                    "ThreadCount": 5,
                    "FailureMaxCount": 10000,
                    "TableFailureMaxCount": 1000,
                    "HandleCollationDiff": false,
                    "ValidationOnly": false,
                    "RecordFailureDelayLimitInMinutes": 0,
                    "SkipLobColumns": false,
                    "ValidationPartialLobSize": 0,
                    "ValidationQueryCdcDelaySeconds": 0,
                    "PartitionSize": 10000
                },
                "PostProcessingRules": null,
                "CharacterSetSettings": null,
                "LoopbackPreventionSettings": null,
                "BeforeImageSettings": null,
                "FailTaskWhenCleanTaskResourceFailed": false
                }
            """,
        )

        blackbelt_glue_database = glue.Database(
            self,
            "blackbelt_glue_database",
            database_name="blackbelt_db",
        )

        glue_role = aws_iam.Role(
            self,
            "glue-role",
            assumed_by=aws_iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSGlueServiceRole"
                ),
                aws_iam.ManagedPolicy.from_aws_managed_policy_name(
                    "AmazonEC2ContainerRegistryFullAccess"
                ),
            ],
        )
        ddk_bucket.grant_read_write(
            glue_role,
        )

        iceberg_glue_job = glue.Job(
            self,
            "iceberg_glue_job",
            executable=glue.JobExecutable.python_etl(
                glue_version=glue.GlueVersion.V3_0,
                script=glue.Code.from_asset("ddk_app/src/glue/iceberg/iceberg_job.py"),
                python_version=glue.PythonVersion.THREE,
                extra_files=None,
                extra_jars=None,
                extra_jars_first=None,
                extra_python_files=None
            ),
            connections=[
                glue.Connection.from_connection_name(
                    self, "IcebergConnection", "Iceberg Connector for Glue 3.0"
                )
            ],
            max_concurrent_runs=50,
            role=glue_role,
            timeout=Duration.minutes(10),
            worker_count=2,
            worker_type=glue.WorkerType.G_1_X,
            spark_ui=glue.SparkUIProps(
                enabled=True,
                bucket=ddk_bucket,
                prefix="sparkHistoryLogs",
            ),
            continuous_logging=glue.ContinuousLoggingProps(
                enabled=True,
            ),
            enable_profiling_metrics=True,
            default_arguments={
                "--iceberg_job_catalog_warehouse": ddk_bucket.s3_url_for_object(
                    "lakehouse/tolldata_raw/"
                ),
                "--TempDir": ddk_bucket.s3_url_for_object("glue_tmp/"),
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-glue-datacatalog": ""
            },
            max_retries=0,
        )

        athena_workgroup = athena.CfnWorkGroup(
            self,
            "athena_workgroup",
            name="athena_workgroup",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                publish_cloud_watch_metrics_enabled=True,
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location="s3://" + ddk_bucket.bucket_name + "/athena_tmp/",
                ),
            ),
        )
