import os
import boto3
import gzip
import io
import pendulum
from airflow import DAG
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


def download_s3_file(bucket_name, key):
    # 从s3下载日志
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        with gzip.GzipFile(fileobj=io.BytesIO(response['Body'].read())) as gz:
            file_content = gz.read().decode('utf-8')
            print(f'EMR Step log content:\n{file_content}')
            # return file_content
    except Exception as e:
        return f'Failed to down load or read file: {str(e)}'


WORK_BUCKET = 'prd-algorithm-data-use1'

# 区域配置
regions_config = [
    {
        "region_name": "us-east-1",
        "bucket": "prd-tauc-use1-data-analysis",
        "aws_conn_id": "aws_conn_use1",
        "start_date": "2024-10-19",
        "end_date": "2024-11-08"
    },
    {
        "region_name": "eu-west-1",
        "bucket": "prd-tauc-euw1-data-analysis",
        "aws_conn_id": "aws_conn_euw1",
        "start_date": "2024-10-19",
        "end_date": "2024-11-08"
    },
    {
        "region_name": "ap-southeast-1",
        "bucket": "prd-tauc-aps1-data-analysis",
        "aws_conn_id": "aws_conn_aps1",
        "start_date": "2024-10-19",
        "end_date": "2024-11-08"
    }
]

SG_use1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_use1.conf'
SG_euw1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_euw1.conf'
SG_aps1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_aps1.conf'
DAG_ID = os.path.basename(__file__).replace('.py', '')
Variable.set(DAG_ID, {
    'bootstrap_path': f's3://{WORK_BUCKET}/emr/emr_job_flow/tauc/emr_bootstrap_sg.bash',
    'log_uri': f's3://{WORK_BUCKET}/emr/emr_log/tauc/{DAG_ID}/',
    'emr_version': 'emr-6.15.0',
    'master_count': 1,
    'master_type': 'm5.xlarge',
    'master_ebs_size_gb': 32,
    'core_count': 1,
    'core_type': 'm6i.xlarge',
    'core_ebs_size_gb': 32,
    'task_count': 1,
    'task_type': 'm6i.xlarge',
    'task_ebs_size_gb': 16,
    'maximum_unit': 8,
    'minimum_unit': 1,
    'maximum_core': 1,
    'maximum_on_demand_unit': 1,
    'idle_timeout': 3600
}, serialize_json=True)

default_args = {
    'owner': 'tauc_bigdata',
    'depends_on_past': False,
    'trigger_rule': 'all_done',
    'queue': 'use1',
    'start_date': pendulum.today('UTC').add(days=-1),
    'email': ['sunhao@tp-link.com.hk'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'tauc_qoe_extract_daily', max_active_runs=1, concurrency=2, default_args=default_args,
    schedule_interval="0 2 * * *")


def run_sensor_and_handle_failure(**kwargs):
    job_flow_id = kwargs['job_flow_id']
    step_id = kwargs['step_id']
    stdout_key = f'emr/emr_log/tauc/{DAG_ID}/{job_flow_id}/steps/{step_id}/stdout.gz'
    stderr_key = f'emr/emr_log/tauc/{DAG_ID}/{job_flow_id}/steps/{step_id}/stderr.gz'
    try:
        sensor = EmrStepSensor(
            task_id='check_step_status',
            job_flow_id=kwargs['job_flow_id'],
            step_id=kwargs['step_id'],
            aws_conn_id="aws_default_use1",
            poke_interval=30,
            timeout=60 * 60,
            dag=dag
        )
        sensor.execute(context=kwargs)
    except:
        download_s3_file(WORK_BUCKET, stdout_key)
        download_s3_file(WORK_BUCKET, stderr_key)
        # 打印错误日志并抛出异常
        raise AirflowException


def get_object(key, bucket_name):
    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return content_object


sg_use1_emr_cluster_config = eval(get_object(SG_use1_EMR_CLUSTER_CONFIG_FILE, WORK_BUCKET))

tauc_bigdata_create_cluster_use1_qoe = EmrCreateJobFlowOperator(
    task_id='tauc_bigdata_create_cluster_use1_qoe',
    job_flow_overrides=sg_use1_emr_cluster_config,
    aws_conn_id='aws_default_use1',
    dag=dag
)

tauc_bigdata_terminate_cluster_use1_qoe = EmrTerminateJobFlowOperator(
    task_id='tauc_bigdata_terminate_cluster_use1_qoe',
    job_flow_id="{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_qoe', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    dag=dag
)


ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_qoe', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'trtree_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tauc-external-tr-gateway_trtree.py'
            ]
        }
    }],
    dag=dag
)

tauc_bigdata_create_cluster_use1_qoe

( tauc_bigdata_create_cluster_use1_qoe)

(tauc_bigdata_create_cluster_use1_qoe >> ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher >>  tauc_bigdata_terminate_cluster_use1_qoe)
