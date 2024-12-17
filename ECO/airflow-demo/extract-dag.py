from airflow import DAG
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from datetime import datetime, timedelta

# 默认参数
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 定义
dag = DAG(
    'tauc_qoe_extract_daily',
    max_active_runs=1,
    concurrency=2,
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2024, 10, 10),
    catchup=False
)

# 自动计算 T-1 日期
t_minus_1 = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# 创建 EMR 集群
create_emr_cluster = EmrCreateJobFlowOperator(
    task_id="create_emr_cluster",
    job_flow_overrides={
        "Name": "daily-spark-emr-cluster",
        "ReleaseLabel": "emr-6.12.0",
        "Applications": [{"Name": "Spark"}],
        "Instances": {
            "MasterInstanceType": "m5.xlarge",
            "SlaveInstanceType": "m5.xlarge",
            "InstanceCount": 3,
            "KeepJobFlowAliveWhenNoSteps": False,
            "TerminationProtected": False,
        },
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "ServiceRole": "EMR_DefaultRole",
    },
    aws_conn_id="aws_default",
    dag=dag,
)

# 添加 Spark 步骤
add_spark_step = EmrAddStepsOperator(
    task_id="add_spark_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    steps=[
        {
            "Name": "Run Spark Job",
            "ActionOnFailure": "CONTINUE",
            "HadoopJarStep": {
                "Jar": "command-runner.jar",
                "Args": [
                    "spark-submit",
                    "--master", "yarn",
                    "--deploy-mode", "cluster",
                    "s3://uat-tauc-aps1-data-analysis/scripts/extract-dist-local.py",
                    "--bucket", "uat-tauc-aps1-data-analysis",
                    "--input_prefix", "source/qoe-raw-batch",
                    "--output_prefix", "dwd",
                    "--start_date", t_minus_1,
                    "--end_date", t_minus_1
                ],
            },
        }
    ],
    dag=dag,
)

# 监控 Spark 步骤
monitor_spark_step = EmrStepSensor(
    task_id="monitor_spark_step",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull(task_ids='add_spark_step', key='return_value')[0] }}",
    aws_conn_id="aws_default",
    dag=dag,
)

# 设置依赖关系
create_emr_cluster >> add_spark_step >> monitor_spark_step