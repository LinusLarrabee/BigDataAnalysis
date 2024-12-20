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

# 配置
WORK_BUCKET = 'prd-algorithm-data-use1'
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
    'tauc_qoe_extract_maintain_multi_region',
    max_active_runs=1,
    concurrency=6,
    default_args=default_args,
    schedule_interval="0 2 * * *",
)

# EMR 配置文件路径
SG_use1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_use1.conf'
SG_euw1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_euw1.conf'
SG_aps1_EMR_CLUSTER_CONFIG_FILE = f'emr/emr_job_flow/tauc/emr_cluster_sg_aps1.conf'


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


# 从 S3 加载 EMR 配置
def get_object(key, bucket_name):
    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return content_object

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

sg_use1_emr_cluster_config = get_object(SG_use1_EMR_CLUSTER_CONFIG_FILE, WORK_BUCKET)
sg_euw1_emr_cluster_config = get_object(SG_euw1_EMR_CLUSTER_CONFIG_FILE, WORK_BUCKET)
sg_aps1_emr_cluster_config = get_object(SG_aps1_EMR_CLUSTER_CONFIG_FILE, WORK_BUCKET)

# us-east-1 任务链
create_cluster_us_east_1 = EmrCreateJobFlowOperator(
    task_id='create_cluster_us_east_1',
    job_flow_overrides=sg_use1_emr_cluster_config,
    aws_conn_id='aws_conn_use1',
    dag=dag
)

add_step_us_east_1 = EmrAddStepsOperator(
    task_id='add_step_us_east_1',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_us_east_1', key='return_value') }}",
    aws_conn_id='aws_conn_use1',
    steps=[
        {
            'Name': 'process_us_east_1',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    's3://prd-tauc-use1-data-analysis/emr/scripts/extract-dist.py',
                    '--bucket', 'prd-tauc-use1-data-analysis',
                    '--input_prefix', 'source/qoe-raw',
                    '--output_prefix', 'dwd',
                    '--start_date', '2024-10-19',
                    '--end_date', '2024-11-08'
                ]
            }
        }
    ],
    dag=dag
)

check_step_us_east_1 = PythonOperator(
    task_id='check_step_us_east_1',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('create_cluster_us_east_1', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='add_step_us_east_1', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

terminate_cluster_us_east_1 = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster_us_east_1',
    job_flow_id="{{ task_instance.xcom_pull('create_cluster_us_east_1', key='return_value') }}",
    aws_conn_id='aws_conn_use1',
    dag=dag
)

create_cluster_us_east_1 >> add_step_us_east_1 >> check_step_us_east_1 >> terminate_cluster_us_east_1

# eu-west-1 任务链
create_cluster_eu_west_1 = EmrCreateJobFlowOperator(
    task_id='create_cluster_eu_west_1',
    job_flow_overrides=sg_euw1_emr_cluster_config,
    aws_conn_id='aws_conn_euw1',
    dag=dag
)

add_step_eu_west_1 = EmrAddStepsOperator(
    task_id='add_step_eu_west_1',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_eu_west_1', key='return_value') }}",
    aws_conn_id='aws_conn_euw1',
    steps=[
        {
            'Name': 'process_eu_west_1',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    's3://prd-tauc-euw1-data-analysis/emr/scripts/extract-dist.py',
                    '--bucket', 'prd-tauc-euw1-data-analysis',
                    '--input_prefix', 'source/qoe-raw',
                    '--output_prefix', 'dwd',
                    '--start_date', '2024-10-19',
                    '--end_date', '2024-11-08'
                ]
            }
        }
    ],
    dag=dag
)

check_step_eu_west_1 = PythonOperator(
    task_id='check_step_eu_west_1',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('create_cluster_eu_west_1', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='add_step_eu_west_1', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

terminate_cluster_eu_west_1 = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster_eu_west_1',
    job_flow_id="{{ task_instance.xcom_pull('create_cluster_eu_west_1', key='return_value') }}",
    aws_conn_id='aws_conn_euw1',
    dag=dag
)

create_cluster_eu_west_1 >> add_step_eu_west_1 >> check_step_eu_west_1 >> terminate_cluster_eu_west_1

# ap-southeast-1 任务链
create_cluster_ap_southeast_1 = EmrCreateJobFlowOperator(
    task_id='create_cluster_ap_southeast_1',
    job_flow_overrides=sg_aps1_emr_cluster_config,
    aws_conn_id='aws_conn_aps1',
    dag=dag
)

add_step_ap_southeast_1 = EmrAddStepsOperator(
    task_id='add_step_ap_southeast_1',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster_ap_southeast_1', key='return_value') }}",
    aws_conn_id='aws_conn_aps1',
    steps=[
        {
            'Name': 'process_ap_southeast_1',
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    's3://prd-tauc-aps1-data-analysis/emr/scripts/extract-dist.py',
                    '--bucket', 'prd-tauc-aps1-data-analysis',
                    '--input_prefix', 'source/qoe-raw',
                    '--output_prefix', 'dwd',
                    '--start_date', '2024-10-19',
                    '--end_date', '2024-11-08'
                ]
            }
        }
    ],
    dag=dag
)

check_step_ap_southeast_1 = PythonOperator(
    task_id='check_step_ap_southeast_1',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('create_cluster_ap_southeast_1', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='add_step_ap_southeast_1', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

terminate_cluster_ap_southeast_1 = EmrTerminateJobFlowOperator(
    task_id='terminate_cluster_ap_southeast_1',
    job_flow_id="{{ task_instance.xcom_pull('create_cluster_ap_southeast_1', key='return_value') }}",
    aws_conn_id='aws_conn_aps1',
    dag=dag
)

create_cluster_ap_southeast_1 >> add_step_ap_southeast_1 >> check_step_ap_southeast_1 >> terminate_cluster_ap_southeast_1