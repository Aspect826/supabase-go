package supabase

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"

	"nhooyr.io/websocket"
)

// Client provides Supabase Realtime access with verbose debug output.
type Client struct {
	URL   string
	Key   string
	conn  *websocket.Conn
	Debug bool
}

func New(baseURL, key string) *Client {
	return &Client{URL: baseURL, Key: key}
}

// Connect opens the websocket connection and prints debug info.
func (c *Client) Connect(ctx context.Context) error {
	wsURL := fmt.Sprintf("%s/realtime/v1/websocket?apikey=%s&vsn=1.0.0",
		c.URL, url.QueryEscape(c.Key))

	if c.Debug {
		fmt.Println("[DEBUG] Dialing:", wsURL)
	}

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
					{"event": "*", "schema": "public", "table": "tasks"},
				},
			},
		},
		"ref": "1",
	}
	data, _ := json.Marshal(join)
	if c.Debug {
		fmt.Println("[DEBUG] Sending join payload:", string(data))
	}
	err = conn.Write(ctx, websocket.MessageText, data)
	if err != nil {
		return err
	}

	return nil
}

// Subscribe starts the read loop with debug output.
func (c *Client) Subscribe(ctx context.Context, handler func(map[string]any)) error {
	if c.conn == nil {
		return fmt.Errorf("no connection")
	}

	go func() {
		for {
			_, msg, err := c.conn.Read(ctx)
			if err != nil {
				fmt.Println("[DEBUG] websocket read error:", err)
				return
			}

			if c.Debug {
				fmt.Println("[DEBUG] RAW message:", string(msg))
			}

			var payload map[string]any
			if json.Unmarshal(msg, &payload) == nil {
				handler(payload)
			}
		}
	}()
	return nil
}

func (c *Client) Disconnect() {
	if c.conn != nil {
		_ = c.conn.Close(websocket.StatusNormalClosure, "normal close")
		c.conn = nil
	}
}
