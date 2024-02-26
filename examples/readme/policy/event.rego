package event

src[s] {
	input.data.kind == "storage#object"
	input.data.bucket == "swarm-test-bucket"
	s := {
		"parser": "json",
		"schema": "my_log",
	}
}
