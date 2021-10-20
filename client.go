package dgraphql

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
)

const ClientTimeout = 20 * time.Second

type Client struct {
	url   string
	token string
}

type Request struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

var tracer = otel.Tracer("dgraph")

func NewClient(url, token string) *Client {
	return &Client{
		url:   url,
		token: token,
	}
}

func (c *Client) GraphQL(ctx context.Context, q string, result interface{}) error {
	ctx, childSpan := tracer.Start(ctx, "query")
	defer childSpan.End()

	qr := Request{
		Query: q,
	}
	buf, err := json.Marshal(qr)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, ClientTimeout)
	defer cancel()

	url := c.url + "/graphql"
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(buf))
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

	er := errorResponse{}
	err = json.Unmarshal(b, &er)
	if err != nil {
		return err
	}
	if len(er.Errors) > 0 {
		return fmt.Errorf(er.Errors[0].Message)
	}

	if result == nil {
		return nil
	}
	return json.Unmarshal(b, result)
}

// Same as above, but with different headers and query string format for DQL
func (c *Client) DQL(ctx context.Context, q string, result interface{}) error {
	ctx, childSpan := tracer.Start(ctx, "query")
	defer childSpan.End()

	ctx, cancel := context.WithTimeout(ctx, ClientTimeout)
	defer cancel()

	url := c.url + "/query"
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(q))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/dql")
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

	er := errorResponse{}
	err = json.Unmarshal(b, &er)
	if err != nil {
		return err
	}
	if len(er.Errors) > 0 {
		return fmt.Errorf(er.Errors[0].Message)
	}

	if result == nil {
		return nil
	}
	return json.Unmarshal(b, result)
}

func (c *Client) RDF(ctx context.Context, q string, result interface{}) error {
	ctx, childSpan := tracer.Start(ctx, "rdf")
	defer childSpan.End()

	ctx, cancel := context.WithTimeout(ctx, ClientTimeout)
	defer cancel()

	url := c.url + "/mutate?commitNow=true"
	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(q))
	if err != nil {
		return err
	}
	req.Header.Add("Content-Type", "application/rdf")
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

	if result == nil {
		return nil
	}
	return json.Unmarshal(b, result)
}
