package dgws

import (
	"encoding/json"
	dgctx "github.com/darwinOrg/go-common/context"
	dgerr "github.com/darwinOrg/go-common/enums/error"
	"github.com/darwinOrg/go-common/result"
	dglogger "github.com/darwinOrg/go-logger"
	ve "github.com/darwinOrg/go-validator-ext"
	"github.com/darwinOrg/go-web/utils"
	"github.com/darwinOrg/go-web/wrapper"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/rolandhe/saber/gocc"
	"net/http"
)

type WebSocketMessage[T any] struct {
	Connection  *websocket.Conn
	MessageType int
	MessageData *T
}

type EndFunc func(mt int, data []byte) bool
type convertMessageFunc[T any] func(*dgctx.DgContext, *websocket.Conn, int, []byte) (*WebSocketMessage[T], error)

func DefaultEndFunc(mt int, _ []byte) bool {
	return mt == websocket.CloseMessage || mt == -1
}

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

func GetJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], endFunc EndFunc) {
	rh.GET(rh.RelativePath, bizHandlerJson(rh, endFunc))
}

func PostJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], endFunc EndFunc) {
	rh.POST(rh.RelativePath, bizHandlerJson(rh, endFunc))
}

func GetBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], endFunc EndFunc) {
	rh.GET(rh.RelativePath, bizHandlerBytes(rh, endFunc))
}

func PostBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], endFunc EndFunc) {
	rh.POST(rh.RelativePath, bizHandlerBytes(rh, endFunc))
}

func bizHandlerJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], endFunc EndFunc) gin.HandlerFunc {
	return bizHandler(rh, endFunc, func(ctx *dgctx.DgContext, conn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[T], error) {
		dglogger.Infof(ctx, "server receive msg: %s", data)
		req := new(T)
		err := json.Unmarshal(data, req)
		if err != nil {
			dglogger.Errorf(ctx, "bind message to struct error: %v", err)
			return nil, err
		}

		err = ve.ValidateDefault(req)
		if err != nil {
			dglogger.Errorf(ctx, "validate request object error: %v", err)
			return nil, err
		}

		return &WebSocketMessage[T]{Connection: conn, MessageType: mt, MessageData: req}, nil
	})
}

func bizHandlerBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], endFunc EndFunc) gin.HandlerFunc {
	return bizHandler(rh, endFunc, func(ctx *dgctx.DgContext, conn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[[]byte], error) {
		dglogger.Infof(ctx, "server receive msg size: %d", len(data))
		return &WebSocketMessage[[]byte]{Connection: conn, MessageType: mt, MessageData: &data}, nil
	})
}

func bizHandler[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], endFunc EndFunc, convertFunc convertMessageFunc[T]) gin.HandlerFunc {
	return func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				c.AbortWithStatusJSON(http.StatusOK, result.FailByDgError[dgerr.DgError](dgerr.SYSTEM_BUSY))
				return
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
			if endFunc == nil {
				endFunc = DefaultEndFunc
			}
			if endFunc(mt, message) {
				dglogger.Infof(ctx, "server receive close message, error: %v", err)
				cn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "end"))
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

			if err != nil {
				dglogger.Errorf(ctx, "server read error: %v", err)
				break
			}

			crt, err := convertFunc(ctx, cn, mt, message)
			if err != nil {
				break
			}

			err = rh.BizHandler(c, ctx, crt)
			if err != nil {
				dglogger.Errorf(ctx, "biz handle message[%s] error: %v", message, err)
			}
		}
	}
}
