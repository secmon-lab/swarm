# Comparisons

BigQuery has multiple ways to ingest data from Cloud Storage. The following is a comparison with `swarm`.

## BigQuery Data Transfer Service

The [BigQuery Data Transfer Service](https://cloud.google.com/bigquery/docs/dts-introduction) is a service that automatically transfers data stored in Cloud Storage to BigQuery. This service is designed to automate data transfer, but it differs from `swarm` in the following ways:

- It does not automatically detect data schemas. You need to define the schema in advance. Additionally, it does not automatically update the schema of the destination table if it changes.
- It does not allow for data manipulation; it only transfers data as is. Also, it does not support dynamically determining the data transfer destination.
- Data transfers are performed at regular intervals. It cannot immediately transfer data when new data is stored in Cloud Storage (at least every 15 minutes).

`swarm` addresses these issues. However, if the schema is predetermined and data needs to be transferred periodically, it is recommended to use the BigQuery Data Transfer Service.

## Load data with schema auto-detection

When using the `load` feature to transfer data from Cloud Storage to BigQuery, BigQuery has a feature called [Schema auto-detection](https://cloud.google.com/bigquery/docs/schema-detect) that automatically detects the schema and creates a table. It can also be used to transfer data to BigQuery using the `bq` command-line tool or the BigQuery API. However, it differs from `swarm` in the following ways:

- It uses a randomly selected sample of up to 500 rows to detect the schema. Therefore, when transferring data with diverse schemas, the schema may not be detected correctly.
- The `load` operation needs to be explicitly executed. It cannot immediately transfer data when new data is stored in Cloud Storage. Additionally, there are [daily execution limits per table](https://cloud.google.com/bigquery/quotas#load_jobs), and it does not support deduplication, requiring the use of wildcards when selecting objects to `load`. This imposes constraints on directory structure, object naming conventions, and careful consideration of `load` execution timing.

This feature is suitable for transferring data with consistent schemas in a one-shot manner. However, when dealing with data with diverse schemas or when data is frequently added, the use of `swarm` is considered beneficial.

## `bigquery.InferSchema` + `bigquery.Inserter`

If you are creating your application to insert data, you can use the `bigquery.InferSchema` and `bigquery.Inserter` features available in the BigQuery Go [client library](https://pkg.go.dev/cloud.google.com/go/bigquery). With these features, you can transfer data stored in Cloud Storage to BigQuery. However, the following issues are commonly encountered:

- `InferSchema` does not support the `any` (`interface{}`) type. Therefore, when dealing with log data provided in JSON format, while the BigQuery schema does not need to be defined, you need to define Go structures yourself.
- The combination of these two features does not provide a way to update the schema of an existing table. To update the schema, you either need to delete and recreate the table or calculate the difference compared to the existing schema and update it.

`swarm` addresses these issues. It supports automatic schema detection, schema updates, data manipulation, and dynamically determining the data transfer destination.
