package event

src[s] {
    input.kind == "storage#object"
    input.bucket == "cloudtrail-logs"
    endswith(input.name, ".log")

    s := {
        "schema": "cloudtrail",
        "parser": "json",
    }
}

src[s] {
    input.kind == "storage#object"
    input.bucket == "cloudtrail-logs"
    endswith(input.name, ".gz")

    s := {
        "schema": "cloudtrail",
        "parser": "json",
        "compress": "gzip",
    }
}
