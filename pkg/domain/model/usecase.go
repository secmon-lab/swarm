package model

type LoadDataRequest struct {
	CSEvent *CloudStorageEvent
}

type LoadRequest struct {
	Source Source
	Object CSObject
}
