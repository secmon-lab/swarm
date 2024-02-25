# swarm [![trivy](https://github.com/m-mizutani/swarm/actions/workflows/trivy.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/trivy.yml) [![test](https://github.com/m-mizutani/swarm/actions/workflows/test.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/test.yml) [![lint](https://github.com/m-mizutani/swarm/actions/workflows/lint.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/lint.yml) [![gosec](https://github.com/m-mizutani/swarm/actions/workflows/gosec.yml/badge.svg)](https://github.com/m-mizutani/swarm/actions/workflows/gosec.yml)

A tool to ingest log data with a dynamic schema from Google Cloud Storage into BigQuery.

![swarm](https://github.com/m-mizutani/swarm/assets/605953/7b7ea371-f99a-4437-a26a-b6669bcffa97)

## Features

- Receive events, such as object storage creation in Cloud Storage, via Pub/Sub and ingest the objects as near real-time log records
- **Automatically detect the schema** of objects and **continuously update the schema** of BigQuery tables
- Control the destination of BigQuery dataset and table for ingestion, and modify data for each record using the [Rego](https://www.openpolicyagent.org/docs/latest/) language
- (To be implemented) Read all objects stored in a Cloud Storage bucket and rebuild the BigQuery table

## Documents

Please refer to the following documents for more details

- [Getting Started](./docs/getting_started.md): How to install and execute `swarm` in local as trial.
- [Specification](./docs/specification.md): The specification of the tool.
- [Deployment](./docs/deployment.md): How to deploy the tool into Google Cloud Platform.
- [Rule](./docs/rule.md): How to write Rego rules for the tool.
- [Comparisons](./docs/comparisons.md): Comparisons with other tools and services.

## License

Apache License 2.0
