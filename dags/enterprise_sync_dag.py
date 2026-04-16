from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_ops',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def slack_failure_notification(context):
    """
    Sends a Slack alert on task failure.
    """
    slack_msg = f"""
            :alert: *Airflow Task Failure* :alert:
            *DAG*: {context.get('task_instance').dag_id}
            *Task*: {context.get('task_instance').task_id}
            *Execution*: {context.get('execution_date')}
            *Logs*: <{context.get('task_instance').log_url}|View Logs>
            """
    alert = SlackWebhookOperator(
        task_id='slack_alert',
        http_conn_id='slack_conn',
        message=slack_msg
    )
    return alert.execute(context=context)

with DAG(
    'enterprise_orchestrator',
    default_args=default_args,
    description='Multi-cloud data lifecycle orchestration (AWS -> Databricks -> Snowflake)',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=slack_failure_notification,
) as dag:

    # 1. Trigger Spark Job on Databricks (Transformation Layer)
    run_spark_pipeline = DatabricksSubmitRunOperator(
        task_id='run_medallion_pipeline',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 2,
        },
        spark_python_task={
            'python_file': 'dbfs:/pipelines/airline_analytics_gold.py',
        },
    )

    # 2. Refresh Snowflake Analytics Tables (Serving Layer)
    refresh_snowflake = SnowflakeOperator(
        task_id='refresh_snowflake_tables',
        snowflake_conn_id='snowflake_default',
        sql="CALL FIN_PROD_DWH.PROCEDURES.REFRESH_ANALYTICS();",
        warehouse='COMPUTE_WH'
    )

    # Workflow Dependency
    run_spark_pipeline >> refresh_snowflake
