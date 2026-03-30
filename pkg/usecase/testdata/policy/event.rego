package event

src contains {
	"schema": "cloudtrail",
	"parser": "json",
} if {
	input.data.kind == "storage#object"
	input.cs.bucket == "cloudtrail-logs"
	endswith(input.cs.name, ".log")
}

src contains {
	"schema": "cloudtrail",
	"parser": "json",
	"compress": "gzip",
} if {
	input.data.kind == "storage#object"
	input.cs.bucket == "cloudtrail-logs"
	endswith(input.cs.name, ".gz")
}
