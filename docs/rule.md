# Rule

With Swarm, you can describe rules using the Rego language. These rules pertain to data schema, events, and authorization.

- Rules related to data processing
  - [Event Rule](#event-rule): Defines how to capture an object when an event, such as object creation, occurs.
  - [Schema Rule](#schema-rule): Defines how to transform data retrieved from objects, specify the destination, and extract necessary parameters.
- Other rules
  - [Authorization Rule](#authorization-rule): Rules for authorizing HTTP requests.

This document explains how to write each type of rule.

- `Input`: Describes the schema of the `input` provided for Rego evaluation.
- `Output`: Explains the variables used to store the results of Rego evaluations.
- `Example`: Demonstrates the types of rules that can be described.

## Event Rule

This rule defines how to capture an object when events, such as object creation, occur. The package name is `event`.

```rego
package event

...
```

### Input


The data obtained by Swarm as a notification event is converted and passed into the `input` for evaluation in Rego. The `input` has the following schema:

- `cs`: (Optional): The field indicates object identity of Cloud Storage
  - `bucket`: (Required, `string`) Specifies the name of the bucket containing the object.
  - `name`: (Required, `string`) Specifies the name of the object.
- `size`: (Optional, `int64`) Specifies the size of the object in bytes. If missing or unknown, it will be omitted.
- `created_at`: (Optional, `int64`) Specifies the Unix timestamp (second) the object was created. If missing or unknown, it will be omitted.
- `digests`: (Optional, `array`) Specifies the hash value of the object.
  - `alg`: (Required, `string`) Specifies the algorithm used for the hash value.
  - `value`: (Required, `string`) Specifies the hash value.
- `data`: (Optional, `object`) Represents the original notification data. This field is used to access the original notification data when necessary.

An example of the `input` is as follows:

```json
{
  "cs": {
    "bucket": "mztn-sample-bucket",
    "name": "mydir/GA1ZivRbQAAAyXs.log"
  },
  "size": 434358,
  "created_at": 1708130907,
  "digests": [
    {
      "alg": "md5",
      "value": "eb9b8a4296628acbbd90ff20065fb9d1"
    },
  ],
  "data": {
    "kind": "storage#object",
    "id": "mztn-sample-bucket/my_logs/GA1ZivRbQAAAyXs.log/1708130907832889",
    "selfLink": "https://www.googleapis.com/storage/v1/b/mztn-sample-bucket/o/mydir%2FGA1ZivRbQAAAyXs.log",
    "name": "mydir/GA1ZivRbQAAAyXs.log",
    "bucket": "mztn-sample-bucket",
    "generation": "1708130907832889",
    "metageneration": "1",
    "contentType": "text/plain",
    "timeCreated": "2024-02-17T00:48:27.868Z",
    "updated": "2024-02-17T00:48:27.868Z",
    "storageClass": "STANDARD",
    "timeStorageClassUpdated": "2024-02-17T00:48:27.868Z",
    "size": "434358",
    "md5Hash": "65uKQpZiisu9kP8gBl+50Q==",
    "mediaLink": "https://storage.googleapis.com/download/storage/v1/b/mztn-sample-bucket/o/mydir%2FGA1ZivRbQAAAyXs.log?generation=1708130907832889&alt=media",
    "crc32c": "Ints+A==",
    "etag": "CLmE97+TsYQDEAE="
  }
}
```

### Output

The result of Rego evaluation creates a set called `src`. This set contains objects with the following schema:

- `parser`: (Required, `"json"`) Specifies the type of parser for parsing the object. Currently, only `json` is supported.
- `schema`: (Required, `string`) Specifies the schema for processing the parsed data. The name specified here is used for evaluating Schema Rules.
- `compress`: (Optional, `string`) Specifies the compression type if the object is compressed. Currently, only `gzip` is supported.
  - Note: If `contentEncoding` is specified as `gzip` in Cloud Storage, the object is automatically decompressed during retrieval, so this parameter is not necessary.

### Example

You can describe rules such as the following. These rules define how to capture `.log.gz` and `.log` files in the `mydir` directory within the `mztn-sample-bucket` bucket.

```rego
package event

# Rule1: Source definition for compressed log files
src[s] {
    input.cs,bucket == "mztn-sample-bucket"
    startswith(input.cs.name, "my_logs/")
    endswith(input.cs.name, ".log.gz")

    s := {
        "parser": "json",
        "schema": "access_log",
        "compress": "gzip",
    }
}

# Rule2: Source definition for uncompressed log files
src[s] {
    input.cs.bucket == "mztn-sample-bucket"
    startswith(input.cs.name, "my_logs/")
    endswith(input.cs.name, ".log")

    s := {
        "parser": "json",
        "schema": "access_log",
    }
}
```

Rule1 is a rule for capturing compressed log files in gzip format, based on the condition that the file suffix is `.log.gz`. On the other hand, Rule2 is a rule for capturing uncompressed log files, based on the condition that the file suffix is `.log`. This allows you to write rules for capturing files in different formats within the same bucket.

Each object is parsed according to the specified `parser` (in this example, `json`), and the result is processed according to the Schema Rule specified by `schema`. This example assumes the existence of a schema named `access_log`.

## Schema Rule

This rule defines how to process each record retrieved from objects. The package name is `schema.{name}`, where `{name}` is the schema name. It must comply with the constraints of Rego package names, containing only alphanumeric characters and underscores (see [Grammar](https://www.openpolicyagent.org/docs/latest/policy-reference/#grammar) for details). This schema name must match the `schema` specified in the Event Rule.

```rego
package schema.access_log

...
```

### Input

The `input` for Rego evaluation is the record resulting from parsing with the `parser` specified in the Event Rule. This will be different for each log.

As a reference, let's consider the following data:

```json
{
    "log_id": "zhT9XEfXopNeTqv9",
    "event_time": 1559347200,
    "remote_ip": "192.168.12.1",
    "user": "user1",
    "action": "login",
    "success": true
}
```

### Output

The result of Rego evaluation creates a set called `log`. This set contains objects with the following schema:

- `dataset`: (Required, `string`) Specifies the BigQuery dataset name to ingest the log. The dataset must be created in advance.
- `table`: (Required, `string`) Specifies the name of the BigQuery table to ingest the log. If the table does not exist, it will be created automatically.
- `partition`: (Optional, `"hour" | "day" | "month" | "year"`) Specifies the granularity for [Time-unit column partitioning](https://cloud.google.com/bigquery/docs/partitioned-tables#date_timestamp_partitioned_tables) for the `Timestamp` field containing the log timestamp. An empty string indicates no Time-unit column partitioning.
  - This option is only available when creating BigQuery tables.
  - A finer granularity improves search efficiency but be mindful of the [constraints](https://cloud.google.com/bigquery/quotas#partitioned_tables) and costs. Refer to [this link](https://cloud.google.com/bigquery/docs/partitioned-tables) for more details.
- `id`: (Optional, `string`) Specifies an ID to ensure the uniqueness of the log. If such a field exists in the original log, its value can be specified. If not, a hash of the combination of the bucket name, object name, and the ordinal number of the log (contained in the object stored in `log`) will be automatically generated.
- `timestamp`: (Required, `float64`) Specifies the log timestamp in Unix Timestamp format. This value can be obtained from fields such as `event_time`.
- `data`: (Required, `object`) Specifies the log data. Normally, this will be the `input` as it is. If you want to modify the values of the original data or remove specific fields, you can specify an object with those changes.

### Example

You can describe rules such as the following. This rule defines a schema named `access_log`.

```rego
package schema.access_log

log[d] {
    d := {
        "dataset": "my_dataset",
        "table": "access_log",
        "id": input.log_id,
        "timestamp": input.event_time,
        "data": input,
    }
}
```

## Authorization Rule

This rule is for authorizing HTTP requests. The package name is `auth`.

```rego
package auth

...
```

This rule allows all requests except for access to `/health`.

### Input

- `method` (string): Contains the HTTP method, such as `GET` or `POST`.
- `path` (string): Contains the HTTP request path starting from `/`.
- `remote` (string): Contains the IP address of the remote connection. This value represents the remote address in TCP/IP, and keep in mind that it may differ from the actual public IP address of the connecting device when going through load balancers, etc.
- `query` (object of array of string): Contains the query parameters.
- `header` (object of array of string): Contains the HTTP headers.
- `body` (string): Contains the body of the HTTP request.

Specifically, data will be in the following format:

```json
{
    "method": "POST",
    "path": "/v1/objects/search",
    "remote": "198.51.100.3",
    "query": {
        "limit": ["20"],
        "offset": ["60"]
    },
    "header": {
        "Authorization": ["Bearer xxxxxxxx"],
        "Content-Type": ["application/json"]
    },
    "body": "{\"query\": \"my object\"}"
}
```

### Output

It returns a boolean value called `deny`. Returning `true` denies the request, while returning `false` allows the request. If no rule is defined, it will be `undefined`, which is treated the same as `false`, allowing all requests.

### Example

You can describe rules such as the following. This rule allows all requests except for access to `/health`.

```rego
package auth

# Deny all requests by default
default deny = true

# If the variable 'allow' is defined, it returns false, allowing the request
deny := false { allow }

# Allow all access to specific paths
allow {
  input.path == "/event/xxx"
}

# Allow requests containing specific tokens in the query
allow {
  input.query.token[_] == "xxxx"
}

# Verify the ID token issued by Google Cloud
jwks_request(url) := http.send({
    "url": url,
    "method": "GET",
    "force_cache": true,
    "force_cache_duration_seconds": 3600 # Cache response for an hour
}).raw_body

allow {
    # Extract token from Authorization header
    authHdr := input.header["Authorization"]
    count(authHdr) == 1
    authHdrValues := split(authHdr[0], " ")
    count(authHdrValues) == 2
    lower(authHdrValues[0]) == "bearer"
    token := authHdrValues[1]

    # Get JWKS of google
    jwks := jwks_request("https://www.googleapis.com/oauth2/v3/certs")

    # Verify token
    io.jwt.verify_rs256(token, jwks)
    claims := io.jwt.decode(token)

    # Allow if the token
    # - is valid
    # - the email address is "my-pubsub@my-project.iam.gserviceaccount.com"
    # - is issued by Google
    # - is not expired
    claims[1]["iss"] == "https://accounts.google.com"
    claims[1]["email"] == "my-pubsub@my-project.iam.gserviceaccount.com"
    time.now_ns() / (1000 * 1000 * 1000) < claims[1]["exp"]
}
```