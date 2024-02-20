package schema.github_audit

logs[log] {
    log := {
        "timestamp": input.timestamp,
        "data": input,
    }
}
