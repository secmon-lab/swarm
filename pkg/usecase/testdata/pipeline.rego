package pipeline

stream[s] {
    input.size != "0"
    input.bucket == "mztn-sample-bucket"
    startswith(input.name, "falcon/")

    s := {
        "schema": "falcon",
        "format": "json",
        "comp": "gzip",

        "dataset": "example_dataset",
        "table": "example_table",
    }
}
