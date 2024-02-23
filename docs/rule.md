# Rule

swarmは、Rego言語を使用してルールを記述することができます。ルールは、データのスキーマ、イベント、認可に関するものです。

- データの処理に関するルール
  - [Event Rule](#event-rule): オブジェクト作成などのイベントが発生した際に、そのオブジェクトをどのように取り込むかを定義をするルールです。
  - [Schema Rule](#schema-rule): オブジェクトから読み込まれたデータを変換したり、投入先を指定したり、必要なパラメータを抽出するためのルールです。
- その他のルール
  - [Authorization Rule](#authorization-rule): HTTPリクエストに対する認可を行うためのルールです。

## Event Rule

オブジェクトの生成などのイベントが発生した際に、そのオブジェクトを取得する方法を定義するルールです。パッケージ名は `event` になります。

```rego
package event

...
```

### Input

swarmが通知イベントとして取得したデータがそのまま `input` に渡されます。例えばCloud Storageでオブジェクトを作成した場合には、以下のようなデータが渡されます。

```json
{
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
```

### Output

Regoの評価結果は `src` というセットを作成します。このセットには以下のスキーマのオブジェクトを格納します。

- `parser`: (Required, `"json"`) オブジェクトをパースするための種別を指定します。現状は `json` のみサポートされています。
- `schema`: (Required, `string`) パースしたデータを処理するためのスキーマを指定します。ここで指定された名前が、Schema Rule の評価に利用されます。
- `compress`: (Optional, `string`) オブジェクトが圧縮されている場合に、その種別を指定します。現状は `gzip` のみサポートされています。
  - 注意：Cloud Storageで `contentEncoding` に `gzip` が指定されている場合はオブジェクトの取得過程で自動的に解凍されるため、このパラメータは不要です。

### Example

例えば下記のようなルールを記述できます。このルールは、`mztn-sample-bucket` バケットにある `mydir` というディレクトリにある `.log.gz` と `.log` ファイルを取り込むためのルールです。

```rego
package event

# Rule1: Source definition for compressed log files
src[s] {
    input.bucket == "mztn-sample-bucket"
    startswith(input.name, "my_logs/")
    endswith(input.name, ".log.gz")

    s := {
        "parser": "json",
        "schema": "access_log",
        "compress": "gzip",
    }
}

# Rule2: Source definition for uncompressed log files
src[s] {
    input.bucket == "mztn-sample-bucket"
    startswith(input.name, "my_logs/")
    endswith(input.name, ".log")

    s := {
        "parser": "json",
        "schema": "access_log",
    }
}
```

Rule1はgzip形式で圧縮してあるログファイルを取り込むためのルールで、ファイルのsuffixが `.log.gz` であることを条件にしています。一方、Rule2はgzip形式で圧縮していないログファイルを取り込むためのルールで、ファイルのsuffixが `.log` であることを条件にしています。このように同じバケットに異なる形式のファイルがあっても、それぞれ異なる方法でファイルを取り込むルールを記述することができます。

各オブジェクトは、`parser` で指定された通り（この例では `json`）にパースされ、その結果は `schema` で指定された Schema Rule に従って処理されます。この例では、`access_log` という名前のスキーマが定義されていることを前提にしています。

## Schema Rule

オブジェクトから読み込まれた各レコードに対して、どのように処理するかを定義するルールです。パッケージ名は `schema.{name}`  になります。`{name}` はスキーマ名で、任意の名前を指定できます。ただしRegoのパッケージ名の制約に従う必要があり、英数字とアンダースコアのみになります（詳しくは[Grammar](https://www.openpolicyagent.org/docs/latest/policy-reference/#grammar)を参照）。このスキーマ名は Event Rule で指定された `schema` と一致する必要があります。

```rego
package schema.access_log

...
```

### Input

Rego評価のための `input` は、Event Rule で指定された `parser` でパースされた結果のレコードになります。これはログ毎に全く異なるものとなります。

今回は参考として、以下のようなデータを想定します。

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

Regoの評価結果は `log` というセットを作成します。このセットには以下のスキーマのオブジェクトを格納します。

- `dataset`: (Required, `string`) ログを投入するBigQueryのデータセット名を指定します。データセットは事前に作成されている必要があります。
- `table`: (Required, `string`) ログを投入するBigQueryのテーブル名を指定します。テーブルは存在しない場合、自動的に作成されます。
- `id`: (Optional, `string`) そのログの一意性を保証するためのIDを指定します。もし元のログにそのようなフィールドが存在している場合は、その値を指定することができます。指定しない場合は、バケット名、オブジェクト名、およびSchema Ruleによって生成されたログ(`log`に格納されたオブジェクト)の順序番号が組み合わされたものをハッシュした値が自動的に生成されます。
- `timestamp`: (Required, `float64`) ログのタイムスタンプをUnix Timestamp形式で指定します。この値は `event_time` などのフィールドから取得することができます。
- `data`: (Required, `object`) ログのデータを指定します。通常は `input` をそのまま指定します。もし元データの値を変更したり、特定のフィールドを削除したい場合は、そのような変更を加えたオブジェクトを指定することができます。

### Example

例えば下記のようなルールを記述できます。このルールは、`access_log` という名前のスキーマを定義しています。

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

HTTPリクエストに対する認可を行うためのルールです。パッケージ名は `auth` になります。

```rego
package auth

...
```

このルールは `/health` へのアクセスを除く、全てのリクエストを許可するルールです。

### Input

