package schema.cloudtrail

log[d] {
    r := input.Records[_]
    d := {
        "dataset": "my_dataset",
        "table": "cloudtrail",
        "timeunit": "month",

        "id": r.eventID,
        "timestamp": time.parse_rfc3339_ns(r.eventTime) / 1000 * 1000 * 1000,
        "data": r,
    }
}
