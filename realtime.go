package supabase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"nhooyr.io/websocket"
)

// Client provides Supabase Realtime access.
type Client struct {
	URL  string
	Key  string
	conn *websocket.Conn
}

// New creates a new realtime client.
func New(baseURL, key string) *Client {
	return &Client{URL: baseURL, Key: key}
}

// Connect opens the websocket connection.
func (c *Client) Connect(ctx context.Context) error {
	wsURL := fmt.Sprintf("%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0",
		c.URL, url.QueryEscape(c.Key))

	conn, _, err := websocket.Dial(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	c.conn = conn

	join := map[string]any{
		"topic": "realtime:public:tasks",
		"event": "phx_join",
		"payload": map[string]any{
			"config": map[string]any{
				"postgres_changes": []map[string]string{
					{
						"event":  "*",
						"schema": "public",
						"table":  "tasks",
					},
				},
			},
		},
		"ref": "1",
	}
	data, _ := json.Marshal(join)
	return conn.Write(ctx, websocket.MessageText, data)
}

// Subscribe subscribes to a Postgres table and listens for all events.
func (c *Client) Subscribe(ctx context.Context, schema, table string, handler func(map[string]any)) error {
	if c.conn == nil {
		return fmt.Errorf("connection not established")
	}

	sub := map[string]any{
		"topic": fmt.Sprintf("realtime:public:%s", table),
		"event": "phx_join",
		"payload": map[string]any{
			"config": map[string]any{
				"postgres_changes": []map[string]string{
					{"event": "*", "schema": schema, "table": table},
				},
			},
		},
		"ref": "2",
	}
	data, _ := json.Marshal(sub)
	if err := c.conn.Write(ctx, websocket.MessageText, data); err != nil {
		return err
	}

	go func() {
		for {
			_, msg, err := c.conn.Read(ctx)
			if err != nil {
				return
			}
			var payload map[string]any
			if json.Unmarshal(msg, &payload) == nil {
				handler(payload)
			}
		}
	}()
	return nil
}

// Disconnect closes the websocket connection.
func (c *Client) Disconnect() {
	if c.conn != nil {
		c.conn.Close(websocket.StatusNormalClosure, "normal closure")
		c.conn = nil
	}
}
