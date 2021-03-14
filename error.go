package dgraphql

type errorResponse struct {
	Errors []errorItem `json:"errors"`
}

type errorItem struct {
	Message string `json:"message"`
}
