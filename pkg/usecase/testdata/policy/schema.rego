package schema.cloudtrail

log contains {
	"dataset": "my_dataset",
	"table": "cloudtrail",
	"timeunit": "month",
	"id": r.eventID,
	"timestamp": ((time.parse_rfc3339_ns(r.eventTime) / 1000) * 1000) * 1000,
	"data": r,
} if {
	r := input.Records[_]
}
