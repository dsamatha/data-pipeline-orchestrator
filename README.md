# Data Pipeline Orchestrator

Enterprise-grade orchestration for cross-platform data pipelines using **Apache Airflow**. This project synchronizes financial and airline datasets across AWS S3, Databricks, and Snowflake.

## Pipeline Observability

Observability is critical for maintaining trust in enterprise data. This orchestrator provides:

- **Task-Level Monitoring**: Every stage of the pipeline (Extraction, Transformation, Loading) is tracked individually.
- **Lineage Tracking**: Clear dependencies between Databricks Spark jobs and Snowflake SQL refreshes.
- **Logging**: Centralized logs for all tasks, accessible via the Airflow UI for rapid debugging.
- **SLA Tracking**: Monitors job duration to ensure data is ready for business stakeholders by the required deadlines.

## Error Handling & Reliability

To ensure the integrity of financial and airline data, we implement several layers of error handling:

1.  **Retry Logic**: Automated retries with exponential backoff for transient failures (e.g., network blips or API rate limits).
2.  **Failure Callbacks**: The `on_failure_callback` function triggers immediate notifications to the engineering team.
3.  **Slack Integration**: Real-time alerts sent to `#data-ops` channel including the specific task failure, DAG ID, and direct links to execution logs.
4.  **ACID Guarantees**: By leveraging Delta Lake and Snowflake, the orchestrator ensures that even if a pipeline step fails, the system remains in a consistent state without partial data loads.

## Tech Stack

- **Orchestrator**: Apache Airflow 2.0+
- **Compute**: Databricks (Spark)
- **Warehouse**: Snowflake
- **Alerting**: Slack / Email

## Setup

1. Configure Airflow Connections:
   - `databricks_default`: Token-based access to your Databricks workspace.
   - `snowflake_default`: Credentials for your Snowflake warehouse.
   - `slack_webhook`: Webhook URL for the Slack notification channel.
2. Deploy the DAGs from `dags/` to your Airflow environment.
3. Set the `enterprise_data_sync` DAG to active.
