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

type InitFunc func(c *gin.Context, ctx *dgctx.DgContext) error
type StartFunc func(ctx *dgctx.DgContext, conn *websocket.Conn) error
type IsEndFunc func(mt int, data []byte) bool
type EndCallbackFunc func(ctx *dgctx.DgContext, conn *websocket.Conn) error
type buildWsMessageFunc[T any] func(ctx *dgctx.DgContext, conn *websocket.Conn, mt int, data []byte) (wsm *WebSocketMessage[T], err error)
type WebSocketMessageCallback[T any] func(ctx *dgctx.DgContext, wsm *WebSocketMessage[T]) error

const (
	WebsocketEndedKey        = "WebsocketEnded"
	ForwardWebsocketEndedKey = "ForwardWebsocketEnded"
	ForwardConnKey           = "ForwardConn"
)

func SetWsEnded(ctx *dgctx.DgContext) {
	ctx.SetExtraKeyValue(WebsocketEndedKey, true)
}

func IsWsEnded(ctx *dgctx.DgContext) bool {
	ended := ctx.GetExtraValue(WebsocketEndedKey)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
}

func SetForwardWsEnded(ctx *dgctx.DgContext) {
	ctx.SetExtraKeyValue(ForwardWebsocketEndedKey, true)
}

func UnsetForwardWsEnded(ctx *dgctx.DgContext) {
	ctx.SetExtraKeyValue(ForwardWebsocketEndedKey, false)
}

func IsForwardWsEnded(ctx *dgctx.DgContext) bool {
	ended := ctx.GetExtraValue(ForwardWebsocketEndedKey)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
}

func SetForwardConn(ctx *dgctx.DgContext, forwardConn *websocket.Conn) {
	ctx.SetExtraKeyValue(ForwardConnKey, forwardConn)
}

func GetForwardConn(ctx *dgctx.DgContext) *websocket.Conn {
	forwardConn := ctx.GetExtraValue(ForwardConnKey)
	if forwardConn == nil {
		return nil
	}

	return forwardConn.(*websocket.Conn)
}

func DefaultStartFunc(_ *dgctx.DgContext, _ *websocket.Conn) error {
	return nil
}

func DefaultIsEndFunc(mt int, _ []byte) bool {
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

func GetJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) {
	rh.GET(rh.RelativePath, bizHandlerJson(rh, initFunc, startFunc, isEndFunc, endCallback))
}

func PostJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) {
	rh.POST(rh.RelativePath, bizHandlerJson(rh, initFunc, startFunc, isEndFunc, endCallback))
}

func GetBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) {
	rh.GET(rh.RelativePath, bizHandlerBytes(rh, initFunc, startFunc, isEndFunc, endCallback))
}

func PostBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) {
	rh.POST(rh.RelativePath, bizHandlerBytes(rh, initFunc, startFunc, isEndFunc, endCallback))
}

func bizHandlerJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) gin.HandlerFunc {
	return bizHandler(rh, initFunc, startFunc, isEndFunc, endCallback, func(ctx *dgctx.DgContext, conn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[T], error) {
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

func bizHandlerBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) gin.HandlerFunc {
	return bizHandler(rh, initFunc, startFunc, isEndFunc, endCallback, func(ctx *dgctx.DgContext, conn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[[]byte], error) {
		dglogger.Debugf(ctx, "server receive msg size: %d", len(data))
		return &WebSocketMessage[[]byte]{Connection: conn, MessageType: mt, MessageData: &data}, nil
	})
}

func bizHandler[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], initFunc InitFunc, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc, buildWsMessage buildWsMessageFunc[T]) gin.HandlerFunc {
	return func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				c.AbortWithStatusJSON(http.StatusOK, result.FailByDgError[dgerr.DgError](dgerr.SYSTEM_BUSY))
				return
			}
			defer semaphore.Release()
		}

		ctx := utils.GetDgContext(c)
		if initFunc != nil {
			err := initFunc(c, ctx)
			if err != nil {
				dglogger.Errorf(ctx, "init error: %v", err)
				c.AbortWithStatusJSON(http.StatusOK, result.SimpleFail[string](err.Error()))
				return
			}
		}

		// 服务升级，对于来到的http连接进行服务升级，升级到ws
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			dglogger.Errorf(ctx, "upgrade error: %v", err)
			c.AbortWithStatusJSON(http.StatusOK, result.SimpleFail[string](err.Error()))
			return
		}
		defer conn.Close()

		if startFunc == nil {
			startFunc = DefaultStartFunc
		}
		err = startFunc(ctx, conn)
		if err != nil {
			dglogger.Errorf(ctx, "start websocket error: %v", err)
			c.AbortWithStatusJSON(http.StatusOK, result.SimpleFail[string](err.Error()))
			return
		}
		defer func() {
			forwardConn := GetForwardConn(ctx)
			if forwardConn != nil {
				forwardConn.Close()
			}
		}()

		if isEndFunc == nil {
			isEndFunc = DefaultIsEndFunc
		}

		for {
			if IsWsEnded(ctx) {
				break
			}

			mt, message, err := conn.ReadMessage()
			if isEndFunc(mt, message) {
				SetWsEnded(ctx)
				dglogger.Infof(ctx, "server receive close message, error: %v", err)
				forwardConn := GetForwardConn(ctx)
				if endCallback != nil && forwardConn != nil {
					err := endCallback(ctx, conn)
					if err != nil {
						dglogger.Errorf(ctx, "end callback error: %v", err)
					}
				}
				break
			}

			if mt == websocket.PingMessage {
				dglogger.Info(ctx, "server receive ping message")
				conn.WriteMessage(websocket.PongMessage, []byte("ok"))
				continue
			}

			if mt == websocket.PongMessage {
				continue
			}

			if err != nil {
				dglogger.Errorf(ctx, "server read error: %v", err)
				break
			}

			wsm, err := buildWsMessage(ctx, conn, mt, message)
			if err != nil {
				break
			}

			err = rh.BizHandler(c, ctx, wsm)
			if err != nil {
				dglogger.Errorf(ctx, "biz handle message[%s] error: %v", message, err)
			}
		}
	}
}
