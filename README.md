# swarm [![trivy](https://github.com/m-mizutani/swarm/actions/workflows/trivy.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/trivy.yml) [![test](https://github.com/m-mizutani/swarm/actions/workflows/test.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/test.yml) [![lint](https://github.com/m-mizutani/swarm/actions/workflows/lint.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/lint.yml) [![gosec](https://github.com/m-mizutani/swarm/actions/workflows/gosec.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/gosec.yml)

A tool to ingest log data with a dynamic schema from Google Cloud Storage into BigQuery.

![swarm](https://github.com/m-mizutani/swarm/assets/605953/7b7ea371-f99a-4437-a26a-b6669bcffa97)

## Features

- Receive events, such as object storage creation in Cloud Storage, via Pub/Sub and ingest the objects as near real-time log records
- **Automatically detect the schema** of objects and **continuously update the schema** of BigQuery tables
- Control the destination of BigQuery dataset and table for ingestion, and modify data for each record using the [Rego](https://www.openpolicyagent.org/docs/latest/) language
- (To be implemented) Read all objects stored in a Cloud Storage bucket and rebuild the BigQuery table

## Rule Examples

In Swarm, rules for saving objects stored in Cloud Storage to BigQuery are described using the Rego language. The rules are divided into two types: **Event rules**, which are executed when receiving the object storage event, and **Schema rules**, which define the format and destination of the log data to be saved in BigQuery.

For detailed description, please refer to the [Rule document](./docs/rule.md).

### Event rule

An Event rule specifies how to ingest data using the [payload data](https://cloud.google.com/storage/docs/json_api/v1/objects#resource-representations) included in the notification from Cloud Storage as the input (`input`).

```rego
package event

src[s] {
	input.data.bucket == "swarm-test-bucket"
	starts_with(input.data.name, "logs/")
	ends_with(input.data.name, ".log.gz")

	s := {
		"parser": "json",
		"schema": "github_audit",
		"compress": "gzip",
	}
}
```

In this example, the rule pertains to objects with the `.log.gz` extension located under the `logs/` prefix in the `swarm-test-bucket` bucket. After being downloaded, these objects are unarchived as gzip files, parsed as JSON, and then saved to BigQuery according to a Schema rule named `github_audit`, assuming they are related to [GitHub Audit Logs](https://docs.github.com/en/enterprise-cloud@latest/admin/monitoring-activity-in-your-enterprise/reviewing-audit-logs-for-your-enterprise/audit-log-events-for-your-enterprise).

### Schema rule

A Schema rule corresponds to the `schema` specified in the Event rule. This rule determines the destination for the parsed object's records, specifies common field data, and performs data transformation. Below is an example for [GitHub Audit Logs](https://docs.github.com/en/enterprise-cloud@latest/rest/enterprise-admin/audit-log?apiVersion=2022-11-28).

```rego
package schema.github_audit

log[d] {
	d := {
		"dataset": "my_log_dataset",
		"table": "my_github_audit",

		"id": input._document_id,
		"timestamp": input["@timestamp"] / 1000,
		"data": json.patch(input, [{"op": "remove", "path": "/@timestamp"}]),
	}
}
```

This rule performs the following operations:

- Saves the data to a BigQuery dataset named `my_log_dataset` and table named `my_github_audit`. Table will be created automatically if it does not exist.
  - While this example always saves to the same dataset and table, it's also possible to change the destination based on the data content.
- The `id` is automatically generated if not specified; however, as GitHub Audit Logs contain a unique ID `_document_id` for each log, it is utilized here, which can help in deduplication in BigQuery.
- The field `@timestamp` is saved in UNIX time in milliseconds, so it is converted to seconds and stored in the `timestamp` field.
- The `@timestamp` field from the original data cannot be used as a field name in BigQuery, so it is removed and saved.

## Documents

Please refer to the following documents for more details

- [Getting Started](./docs/getting_started.md): How to install and execute `swarm` in local as trial.
- [Specification](./docs/specification.md): The specification of the tool.
- [Deployment](./docs/deployment.md): How to deploy the tool into Google Cloud Platform.
- [Rule](./docs/rule.md): How to write Rego rules for the tool.
- [Comparisons](./docs/comparisons.md): Comparisons with other tools and services.

## License

Apache License 2.0
