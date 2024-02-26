package auth

default deny = true

deny = false {
	allow
}

allow {
	input.header.Authorization[_] == "Bearer good-token"
}
