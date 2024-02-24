package auth

default deny = true

deny = false {
	allow
}

allow {
	print(input.header)
	input.header.Authorization[_] == "Bearer good-token"
}
