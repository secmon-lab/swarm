package event

src[s] {
  input.kind == "storage#object"
  input.bucket == "swarm-test-bucket"
  s := {
    "parser": "json",
    "schema": "my_log",
  }
}
