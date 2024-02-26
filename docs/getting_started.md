# Getting Started

First, let's try running it from the local environment. For actual deployment, please refer to [Deployment](docs/deployment.md). Normally, Swarm processes events such as object creation from Cloud Storage via Pub/Sub, but this time we will execute a command to directly specify the objects already on Cloud Storage and ingest them into BigQuery.

## Prerequisites

- Go 1.22 or later
- Google Cloud project (e.g., `my-project`)
- Objects of log files stored in Cloud Storage (e.g., `gs://swarm-test-bucket/test.log`)
  - You can use [sample log files](../examples/readme/data/test.log)
- Target dataset for log insertion in BigQuery (e.g., `my_dataset`)
- Account with permission to read objects from Cloud Storage and ingest data into BigQuery
    1. Authenticate a user with the required permissions using the [gcloud](https://cloud.google.com/sdk/gcloud) command
    2. Create a Service Account and its key file, then set the path to that file in the environment variable `GOOGLE_APPLICATION_CREDENTIALS`

## Installation

```bash
go install github.com/m-mizutani/swarm@latest
```

## Create rule files

`swarm` allows you to write rules using the [Rego](https://www.openpolicyagent.org/docs/latest/policy-language/) language. At a minimum, you need to write "Event rule" and "Schema rule". For more details, please refer to [Rule](./docs/rule.md). First, create files like the following:

**policy/event.rego**
This file describes the rules for reading objects stored in Cloud Storage. The following rules specify that logs are saved in JSON format and use a Schema Rule named `my_schema`.

```rego
package event

src[s] {
  input.data.kind == "storage#object"
  input.cs.bucket == "swarm-test-bucket"
  s := {
    "parser": "json",
    "schema": "my_log",
  }
}
```

**policy/schema.rego**
This file describes how to process the records obtained by parsing the objects.

```rego
package schema.my_log

log[d] {
    d := {
        "dataset": "my_dataset",
        "table": "my_log_table",

        "id": input.log_id,
        "timestamp": input.event_time,
        "data": input,
    }
}
```

## Execution

Once the rules are prepared, execute the command as follows:

```bash
swarm exec --bigquery-project-id my-project -p ./policy gs://swarm-test-bucket/test.log | jq
```

If successful, this command will output logs like the following:

```json
{
  "time": "2024-02-23T11:19:39.960624Z",
  "level": "INFO",
  "msg": "request handled",
  <--- snip --->
  "eventLog": {
    "ID": "b70aa4ae-69e6-45f2-bea8-16aa148132d6",
    "CSBucket": "swarm-test-bucket",
    "CSObjectID": "test.log",
    "StartedAt": "2024-02-23T11:27:59.407931Z",
    "FinishedAt": "2024-02-23T11:28:01.122652Z",
    "Success": true,
    "Ingests": [
      {
        "ID": "0b10e152-f19e-467a-96d1-64ad24db6985",
        "StartedAt": "2024-02-23T11:27:59.57609Z",
        "FinishedAt": "2024-02-23T11:28:01.12265Z",
        "ObjectSchema": "my_log",
        "DatasetID": "swarm_test",
        "TableID": "my_log_table",
        "TableSchema": "[{\"name\":\"ID\",\"type\":\"STRING\"},{\"name\":\"IngestID\",\"type\":\"STRING\"},{\"name\":\"Timestamp\",\"type\":\"TIMESTAMP\"},{\"name\":\"InsertedAt\",\"type\":\"TIMESTAMP\"},{\"fields\":[{\"name\":\"user\",\"type\":\"STRING\"},{\"name\":\"action\",\"type\":\"STRING\"},{\"name\":\"event_time\",\"type\":\"FLOAT\"},{\"name\":\"log_id\",\"type\":\"STRING\"},{\"name\":\"remote_ip\",\"type\":\"STRING\"},{\"name\":\"success\",\"type\":\"BOOLEAN\"}],\"name\":\"Data\",\"type\":\"RECORD\"}]",
        "LogCount": 2,
        "Success": true
      }
    ],
    "Error": ""
  }
}
```

You can check if the data has been inserted into BigQuery. The specified `my_dataset` should have a table called `my_log_table` created, and the data should have been inserted. The following schema will be generated:

![](./images/readme/bq_schema.png)

You can check the data with `SELECT * FROM my_dataset.my_log_table`:

![](./images/readme/bq_result.png)
