package cairn

import (
	"net/http"

	"github.com/labstack/echo/v4"
)

type Client struct {
	Port int
	Host string
}

func NewClient() (*Client, error) {
	n := &Client{
		Port: 5432,
		Host: "localhost",
	}

	return n, nil
}

func handleProduce(c echo.Context) error {
	m := c.QueryParam("msg")
	msg := Message{
		Value:      m,
		BytesValue: []byte(m),
		Length:     len([]byte(m)),
	}
	err := logWrite(&msg)
	if err != nil {
		panic(err)
	}
	return c.JSON(http.StatusOK, echo.Map{"status": "OK"})
}
