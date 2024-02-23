package schema.my_log

log[d] {
    d := {
        "dataset": "swarm_test",
        "table": "my_log_table",

        "id": input.log_id,
        "timestamp": input.event_time,
        "data": input,
    }
}
