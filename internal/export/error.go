package export

import (
	"encoding/json"
	"fmt"
	"io"
)

type errorObject struct {
	Schema int         `json:"schema"`
	Error  errorDetail `json:"error"`
}

type errorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// WriteError reports a failure to w: one JSON object in json mode, one plain line otherwise.
func WriteError(w io.Writer, format Format, code string, err error) {
	if format == FormatJSON {
		encoder := json.NewEncoder(w)
		encoder.SetEscapeHTML(false)
		// A failed write to stderr leaves nothing else to report to.
		_ = encoder.Encode(errorObject{Schema: Schema, Error: errorDetail{Code: code, Message: err.Error()}})
		return
	}
	fmt.Fprintf(w, "asbctl: %v\n", err)
}
