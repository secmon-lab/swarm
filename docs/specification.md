# Specification

The swarm has several subcommands, each with the following details:

- `serve`: Launches an HTTP server to subscribe to Pub/Sub topics and receive notifications for objects stored in Cloud Storage. It reads the objects indicated by the notifications and saves them to BigQuery.
- `ingest`: Reads and saves objects stored in Cloud Storage directly to BigQuery in a one-shot manner, primarily used for debugging purposes.
- `client`: Assists in interacting with the HTTP server launched by the `serve` subcommand.
- `retry`: Re-executes failed processes due to errors.

## serve mode

Upon startup, the following endpoints are available:

- `GET /health`: Checks the server's status. If the server is operating normally, it returns `200 OK`.
- `POST /event/pubsub/cs`: Receives notifications from Pub/Sub, specifically notifications for object creation in Cloud Storage.
- `POST /event/pubsub/swarm`: Receives messages that swarm creates from Pub/Sub
