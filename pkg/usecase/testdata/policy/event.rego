package event

src[{
	"schema": "cloudtrail",
	"parser": "json",
}] {
	input.kind == "storage#object"
	input.bucket == "cloudtrail-logs"
	endswith(input.name, ".log")
}

src[{
	"schema": "cloudtrail",
	"parser": "json",
	"compress": "gzip",
}] {
	input.kind == "storage#object"
	input.bucket == "cloudtrail-logs"
	endswith(input.name, ".gz")
}
