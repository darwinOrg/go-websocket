package dgws_test

import (
	"encoding/json"
	"fmt"
	dgctx "github.com/darwinOrg/go-common/context"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/darwinOrg/go-monitor"
	"github.com/darwinOrg/go-web/wrapper"
	dgws "github.com/darwinOrg/go-websocket"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"net/url"
	"testing"
	"time"
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
	path := "/echo"
	engine := wrapper.DefaultEngine()
	dgws.Get(&wrapper.RequestHolder[dgws.WebSocketMessage, error]{
		RouterGroup: engine.Group(path),
		BizHandler: func(_ *gin.Context, ctx *dgctx.DgContext, wsm *dgws.WebSocketMessage) error {
			dglogger.Infof(ctx, "handle message: %s", string(wsm.MessageData))
			return nil
		},
	}, &dgws.WebSocketHandlerConfig{
		BizKey: "bizId",
		GetBizIdHandler: func(c *gin.Context) string {
			return c.Query("bizId")
		},
		StartHandler:       nil,
		IsEndedHandler:     dgws.DefaultIsEndHandler,
		EndCallbackHandler: nil,
	})
	go engine.Run(fmt.Sprintf(":%d", 8080))
	time.Sleep(time.Second * 3)

	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	sendMessage(ctx, "localhost:8080", path, datas, 5)
}

func TestSendLocal(t *testing.T) {
	dgws.InitWsConnLimit(10)
	path := "/public/v1/ws/test"
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	sendMessage(ctx, "localhost:9090", path, datas, 5)
}

func TestSendProd(t *testing.T) {
	dgws.InitWsConnLimit(10)
	path := "/ground/public/v1/ws/test"
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	sendMessage(ctx, "e.globalpand.cn", path, datas, 5)
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
