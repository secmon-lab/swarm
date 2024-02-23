package event

src[s] {
    input.size != "0"
    input.bucket == "mztn-sample-bucket"
    startswith(input.name, "test/")

    s := {
        "schema": "my_app",
        "parser": "json",
        # "comp": "gzip",
    }
}
