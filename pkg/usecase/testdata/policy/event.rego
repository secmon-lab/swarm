package event

src[{
	"schema": "cloudtrail",
	"parser": "json",
}] {
	input.data.kind == "storage#object"
	input.cs.bucket == "cloudtrail-logs"
	endswith(input.cs.name, ".log")
}

src[{
	"schema": "cloudtrail",
	"parser": "json",
	"compress": "gzip",
}] {
	input.data.kind == "storage#object"
	input.cs.bucket == "cloudtrail-logs"
	endswith(input.cs.name, ".gz")
}
