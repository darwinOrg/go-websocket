package dgws

import (
	"encoding/json"
	dglogger "github.com/darwinOrg/go-logger"
	ve "github.com/darwinOrg/go-validator-ext"
	"github.com/darwinOrg/go-web/utils"
	"github.com/darwinOrg/go-web/wrapper"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/rolandhe/saber/gocc"
	"net/http"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// 根据鉴权的方式来处理, 如果不想鉴权的就直接返回true, 如果需要鉴权就要根据判断来返回true，或者false
		return true
	},
}

var semaphore gocc.Semaphore

func InitWsConnLimit(limit uint) {
	semaphore = gocc.NewDefaultSemaphore(limit)
}

func SetCheckOrigin(checkOriginFunc func(r *http.Request) bool) {
	upgrader.CheckOrigin = checkOriginFunc
}

func Get[T any](rh *wrapper.RequestHolder[T, error]) {
	rh.GET(rh.RelativePath, bizHandler(rh))
}

func Post[T any](rh *wrapper.RequestHolder[T, error]) {
	rh.POST(rh.RelativePath, bizHandler(rh))
}

func bizHandler[T any](rh *wrapper.RequestHolder[T, error]) gin.HandlerFunc {
	return func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				panic("request too fast")
			}
			defer semaphore.Release()
		}

		// 服务升级，对于来到的http连接进行服务升级，升级到ws
		cn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			panic(err)
		}

		ctx := utils.GetDgContext(c)
		defer func(wc *websocket.Conn) {
			err := wc.Close()
			if err != nil {
				dglogger.Errorf(ctx, "close websocket conn error: %v", err)
			}
		}(cn)

		for {
			mt, message, err := cn.ReadMessage()
			if mt == websocket.CloseMessage {
				dglogger.Info(ctx, "server receive close message")
				break
			}

			if mt == websocket.PingMessage {
				dglogger.Info(ctx, "server receive ping message")
				cn.WriteMessage(websocket.PongMessage, []byte("ok"))
				continue
			}

			if mt == websocket.PongMessage {
				continue
			}

			if mt == websocket.BinaryMessage {
				dglogger.Errorf(ctx, "server not support binary message")
				continue
			}

			if err != nil {
				dglogger.Errorf(ctx, "server read error: %v", err)
				break
			}

			dglogger.Infof(ctx, "server receive msg: %s", message)
			req := new(T)
			err = json.Unmarshal(message, req)
			if err != nil {
				dglogger.Errorf(ctx, "bind message to struct error: %v", err)
				break
			}

			if err := c.ShouldBind(req); err != nil {
				errMsg := ve.TranslateValidateError(err, ctx.Lang)
				dglogger.Errorf(ctx, "bind request object error: %s", errMsg)
				break
			}

			err = rh.BizHandler(c, ctx, req)
			if err != nil {
				dglogger.Errorf(ctx, "biz handle message[%s] error: %v", message, err)
			}
		}
	}
}
