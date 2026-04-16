# Data Pipeline Orchestrator

This project manages the end-to-end data lifecycle for enterprise datasets across a multi-cloud environment (**AWS**, **Databricks**, and **Snowflake**) using **Apache Airflow**.

## Data Lifecycle Management

Airflow serves as the central nervous system for our data platform, orchestrating the following flow:

1.  **Ingestion (AWS S3)**: Monitoring and triggering extraction processes from S3 landing zones.
2.  **Transformation (Databricks/Spark)**: Orchestrating complex Medallion Architecture pipelines (Bronze/Silver/Gold) via the `DatabricksSubmitRunOperator`.
3.  **Serving (Snowflake)**: Finalizing the lifecycle by refreshing analytics-ready tables in Snowflake using the `SnowflakeOperator`.

## Error Handling & Notifications

Reliability is built into every layer of the orchestration:

-   **Retry Strategy**: Tasks are configured with automated retries and a 5-minute delay to handle transient cloud connectivity issues.
-   **Slack Notifications**: In the event of a critical failure, a global `on_failure_callback` triggers a Slack webhook. This sends an immediate alert to the engineering team with the specific DAG/Task ID and a direct link to the Airflow logs for rapid triage.
-   **Email Alerts**: Secondary notifications are sent via email to ensure visibility across the organization.

## Project Structure

- `dags/`: Contains the `enterprise_orchestrator` DAG definitions.
- `plugins/`: Custom operators and sensors.

## Tech Stack

- **Orchestration**: Apache Airflow
- **Cloud Storage**: AWS S3
- **Distributed Compute**: Databricks (Spark)
- **Data Warehouse**: Snowflake
- **Monitoring**: Slack API
