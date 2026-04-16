from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def on_failure_callback(context):
    """
    Custom callback to send Slack notifications on DAG failure.
    """
    slack_msg = f"""
            :red_circle: Task Failed. 
            *Task*: {context.get('task_instance').task_id}  
            *Dag*: {context.get('task_instance').dag_id} 
            *Execution Time*: {context.get('execution_date')}  
            *Log Url*: {context.get('task_instance').log_url} 
            """
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg
    )
    return failed_alert.execute(context=context)

with DAG(
    'enterprise_data_sync',
    default_args=default_args,
    description='Orchestrate cross-platform data sync between Databricks and Snowflake',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    on_failure_callback=on_failure_callback,
    tags=['production', 'medallion', 'snowflake', 'databricks'],
) as dag:

    # 1. Trigger the Databricks Spark Job (Medallion Pipeline)
    # This configuration submits a one-time run to a Databricks cluster
    trigger_spark_pipeline = DatabricksSubmitRunOperator(
        task_id='trigger_airline_medallion_spark_job',
        databricks_conn_id='databricks_default',
        new_cluster={
            'spark_version': '13.3.x-scala2.12',
            'node_type_id': 'i3.xlarge',
            'num_workers': 2,
        },
        spark_python_task={
            'python_file': 'dbfs:/pipelines/airline_medallion_pipeline.py',
        },
    )

    # 2. Refresh Snowflake Analytics Tables
    # Runs SQL to merge silver/gold data into production schemas
    refresh_snowflake_gold_tables = SnowflakeOperator(
        task_id='refresh_snowflake_analytics',
        snowflake_conn_id='snowflake_default',
        sql="""
            CALL FIN_PROD_DWH.PROCEDURES.REFRESH_GOLD_ANALYTICS();
        """,
        warehouse='COMPUTE_WH',
        database='FIN_PROD_DWH',
        schema='GOLD'
    )

    # Define task dependencies
    trigger_spark_pipeline >> refresh_snowflake_gold_tables
