package dgws_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/darwinOrg/go-monitor"
	dgotel "github.com/darwinOrg/go-otel"
	"github.com/darwinOrg/go-web/wrapper"
	dgws "github.com/darwinOrg/go-websocket"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
)

type testData struct {
	Content string `json:"content" binding:"required,minLength=3"`
}

var datas = []testData{
	{
		Content: "123",
	},
	{
		Content: "456",
	},
	{
		Content: "789",
	},
	{
		Content: "101",
	},
}

func TestSendOwn(t *testing.T) {
	dgws.InitWsConnLimit(10)
	monitor.Start("test", 19002)
	cleanup := initTracer()
	defer cleanup()

	engine := wrapper.DefaultEngine()
	path := "/public/v1/ws/test"
	dgws.Get(&wrapper.RequestHolder[dgws.WebSocketMessage, error]{
		RouterGroup:  engine.Group(""),
		RelativePath: path,
		NonLogin:     true,
		EnableTracer: true,
		BizHandler: func(_ *gin.Context, ctx *dgctx.DgContext, wsm *dgws.WebSocketMessage) error {
			dglogger.Infof(ctx, "handle message: %s", string(wsm.MessageData))
			return nil
		},
	}, &dgws.WebSocketHandlerConfig{
		BizKey: "bizId",
		GetBizIdHandler: func(c *gin.Context) string {
			return c.Query("bizId")
		},
		StartHandler:        nil,
		IsEndedHandler:      dgws.DefaultIsEndHandler,
		EndCallbackHandler:  nil,
		EnableMessageTracer: true,
		PingPeriod:          time.Second * 3,
	})
	go engine.Run(fmt.Sprintf(":%d", 8080))
	time.Sleep(time.Second * 3)

	ctx := dgctx.SimpleDgContext()
	sendMessage(ctx, "localhost:8080", path, datas, 3)
	time.Sleep(time.Second * 5)
}

func TestSendLocal(t *testing.T) {
	dgws.InitWsConnLimit(10)
	path := "/public/v1/ws/test"
	ctx := dgctx.SimpleDgContext()
	sendMessage(ctx, "localhost:8080", path, datas, 3)
}

func TestSendProd(t *testing.T) {
	dgws.InitWsConnLimit(10)
	path := "/ground/public/v1/ws/test"
	ctx := dgctx.SimpleDgContext()
	sendMessage(ctx, "e.xjob.co", path, datas, 3)
}

func initTracer() func() {
	exporter, err := otlptracehttp.New(
		context.Background(),
		otlptracehttp.WithEndpoint("localhost:4318"),
		otlptracehttp.WithInsecure(),
	)
	if err != nil {
		panic(err)
	}

	cleanup, err := dgotel.InitTracer("test-service", exporter)
	if err != nil {
		panic(err)
	}

	return cleanup
}

func sendMessage(ctx *dgctx.DgContext, host string, path string, datas []testData, intervalSeconds time.Duration) {
	u := url.URL{Scheme: "ws", Host: host, Path: path}
	dglogger.Infof(ctx, "client connecting to %s", u.String())

	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	if err != nil {
		dglogger.Fatalf(ctx, "dial server: %v", err)
	}
	defer c.Close()

	for _, data := range datas {
		body, _ := json.Marshal(data)
		err := c.WriteMessage(websocket.TextMessage, body)
		if err != nil {
			dglogger.Errorf(ctx, "client write error: %v", err)
			return
		}
		time.Sleep(time.Second * intervalSeconds)
	}

	err = c.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "end"))
	if err != nil {
		dglogger.Errorf(ctx, "client write close error: %v", err)
		return
	}
}
