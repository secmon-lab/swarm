package auth

default deny = true

deny = false if {
	allow
}

allow if {
	input.header.Authorization[_] == "Bearer good-token"
}
