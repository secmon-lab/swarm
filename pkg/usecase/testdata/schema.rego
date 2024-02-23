package schema.my_app

log[d] {
    d := {
        "dataset": "my_dataset"
        "table": "my_table"
        "timeunit": "month"

        "id": input.id
        "timestamp": input.timestamp,
        "data": input,
    }
}
