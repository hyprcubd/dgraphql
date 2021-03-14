package dgraphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"go.opentelemetry.io/otel"
)

// Client keeps track of the dgraph server and auth token
type Client struct {
	url   string
	token string
}

type request struct {
	Query string `json:"query"`
}

var tracer = otel.Tracer("dgraphql")

// New creates a new client
func New(url, token string) *Client {
	return &Client{
		url:   url,
		token: token,
	}
}

// RaQuery executes the query passed in as-is and unmarshals the result into the result arg
func (c *Client) RawQuery(ctx context.Context, q string, result interface{}) error {
	// This is a no-op if no tracer is set up
	_, span := tracer.Start(ctx, "query")
	defer span.End()

	qr := request{
		Query: q,
	}
	buf, err := json.Marshal(qr)
	if err != nil {
		return err
	}

	req, err := http.NewRequest("POST", c.url, bytes.NewReader(buf))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/json")
	req.Header.Add("Dg-Auth", c.token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Dgraph always returns a 200 even for errors
	if resp.StatusCode != 200 {
		return fmt.Errorf("db returned non 200 code: %d", resp.StatusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	// Convert Dgraph errors into an error
	er := errorResponse{}
	err = json.Unmarshal(b, &er)
	if err != nil {
		return err
	}
	if len(er.Errors) > 0 {
		return fmt.Errorf(er.Errors[0].Message)
	}

	// If user didn't provide an output structure
	if result == nil {
		return nil
	}
	return json.Unmarshal(b, result)
}
