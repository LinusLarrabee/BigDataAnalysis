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

tauc_bigdata_create_cluster_use1_mongodb = EmrCreateJobFlowOperator(
    task_id='tauc_bigdata_create_cluster_use1_mongodb',
    job_flow_overrides=sg_use1_emr_cluster_config,
    aws_conn_id='aws_default_use1',
    dag=dag
)

tauc_bigdata_terminate_cluster_use1 = EmrTerminateJobFlowOperator(
    task_id='tauc_bigdata_terminate_cluster_use1',
    job_flow_id="{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_datax = BashOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tauc-external-tr-gateway',
        'table': 'device',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tauc-external-tr-gateway',
        'table': 'device',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_datax = BashOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tauc-external-tr-gateway',
        'table': 'trtree',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tauc-external-tr-gateway',
        'table': 'trtree',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'webhook_event_record',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'webhook_event_record',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'isp_log_notification_config',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'isp_log_notification_config',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'network_alert',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'network_alert',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'open_api_fail_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'open_api_fail_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'isp_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'isp_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'staff_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'staff_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'webhook_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_activity',
        'table': 'webhook_log',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_statistic',
        'table': 'custom_dashboard_setting',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_statistic',
        'table': 'custom_dashboard_setting',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_statistic',
        'table': 'time_machine_cache',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc_statistic',
        'table': 'time_machine_cache',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'pre-configuration',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'pre-configuration',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'onboardrecord',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'onboardrecord',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'devices',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'devices',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'refreshTimeByUI',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'refreshTimeByUI',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'kpsubscriptions',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'kpsubscriptions',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'tasks',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'tasks',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'component_negotiation',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'component_negotiation',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'kpchanges',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'kpchanges',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_datax = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_datax',
    bash_command="""
        python {{params.python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'python_script': '/home/datax-airflow/tauc_bigdata/sync_run_datax.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'blacklist',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_encrypt = BashOperator(
    task_id='ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_encrypt',
    bash_command="""
        python {{params.encrypt_python_script}} \
            --dst_database {{params.dst_database}} \
            --database_type {{params.database_type}} \
            --database {{params.database}} \
            --table {{params.table}} \
            --src_region {{params.src_region}} \
            --dst_region {{params.dst_region}} \
            --is_sync {{params.is_sync}}
    """,
    params={
        'encrypt_python_script': '/home/datax-airflow/tauc_bigdata/sync_encrypt.py',
        'dst_database': 'tauc',
        'database_type': 'mongodb',
        'database': 'tpuc-traccess',
        'table': 'blacklist',
        'src_region': 'use1',
        'dst_region': 'use1',
        'is_sync': 'True'
    },
    queue='use1',
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
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

ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_check = PythonOperator(
    task_id='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc-external-tr-gateway_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'isp_log_notification_config_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_isp_log_notification_config.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'network_alert_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_network_alert.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'open_api_fail_log_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_open_api_fail_log.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'isp_log_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_isp_log.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'staff_log_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_staff_log.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'webhook_log_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc_activity_webhook_log.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'pre-configuration_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc-traccess_pre-configuration.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc-traccess_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'devices_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc-traccess_devices.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_traccess_devices_df_tpuc-traccess_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'kpsubscriptions_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc-traccess_kpsubscriptions.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc-traccess_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_cipher = EmrAddStepsOperator(
    task_id='ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_cipher',
    job_flow_id="{{ task_instance.xcom_pull(task_ids='tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
    aws_conn_id='aws_default_use1',
    steps=[{
        'Name': 'blacklist_use1_cipher',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                's3://prd-algorithm-data-use1/emr/emr_job_flow/tauc/sqls/ods/encrypt/table/spark_python/use1_use1_tpuc-traccess_blacklist.py'
            ]
        }
    }],
    dag=dag
)

ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_check = PythonOperator(
    task_id='ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_check',
    python_callable=run_sensor_and_handle_failure,
    op_kwargs={
        'job_flow_id': "{{ task_instance.xcom_pull('tauc_bigdata_create_cluster_use1_mongodb', key='return_value') }}",
        'step_id': "{{ task_instance.xcom_pull(task_ids='ods_mongodb_tpuc_traccess_blacklist_df_tpuc-traccess_use1_cipher', key='return_value')[0] }}"
    },
    provide_context=True,
    retries=1,
    dag=dag
)

ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_datax >> ods_mongodb_tauc_external_tr_gateway_device_df_tauc_external_tr_gateway_use1_encrypt >> ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_datax >> ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_encrypt >> ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_webhook_event_record_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_datax >> ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_encrypt >> ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_datax >> ods_mongodb_tpuc_statistic_custom_dashboard_setting_df_tpuc_statistic_use1_encrypt >> ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_datax >> ods_mongodb_tpuc_statistic_time_machine_cache_df_tpuc_statistic_use1_encrypt >> ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_onboardrecord_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_refreshtimebyui_df_tpuc_traccess_use1_encrypt >> tauc_bigdata_create_cluster_use1_mongodb

ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_tasks_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_component_negotiation_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_kpchanges_df_tpuc_traccess_use1_encrypt >> ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_datax >> ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_encrypt >> tauc_bigdata_create_cluster_use1_mongodb

tauc_bigdata_create_cluster_use1_mongodb >> ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_cipher >> ods_mongodb_tauc_external_tr_gateway_trtree_df_tauc_external_tr_gateway_use1_check >> ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_isp_log_notification_config_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_network_alert_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_open_api_fail_log_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_isp_log_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_staff_log_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_cipher >> ods_mongodb_tpuc_activity_webhook_log_df_tpuc_activity_use1_check >> ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_cipher >> ods_mongodb_tpuc_traccess_pre_configuration_df_tpuc_traccess_use1_check >> ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_cipher >> ods_mongodb_tpuc_traccess_devices_df_tpuc_traccess_use1_check >> ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_cipher >> ods_mongodb_tpuc_traccess_kpsubscriptions_df_tpuc_traccess_use1_check >> ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_cipher >> ods_mongodb_tpuc_traccess_blacklist_df_tpuc_traccess_use1_check >> tauc_bigdata_terminate_cluster_use1
