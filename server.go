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
	ForwardConn *websocket.Conn
	MessageType int
	MessageData *T
}

type StartFunc func(ctx *dgctx.DgContext, conn *websocket.Conn) (forwardConn *websocket.Conn, err error)
type IsEndFunc func(mt int, data []byte) bool
type ForwardCallbackFunc func(ctx *dgctx.DgContext, forwardConn *websocket.Conn) error
type buildWsMessageFunc[T any] func(ctx *dgctx.DgContext, conn *websocket.Conn, forwardCOnn *websocket.Conn, mt int, data []byte) (wsm *WebSocketMessage[T], err error)

const endedKey = "WS_ENDED"

func IsWsEnded(ctx *dgctx.DgContext) bool {
	ended := ctx.GetExtraValue(endedKey)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
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

func GetJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) {
	rh.GET(rh.RelativePath, bizHandlerJson(rh, startFunc, startCallback, isEndFunc, endCallback))
}

func PostJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) {
	rh.POST(rh.RelativePath, bizHandlerJson(rh, startFunc, startCallback, isEndFunc, endCallback))
}

func GetBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) {
	rh.GET(rh.RelativePath, bizHandlerBytes(rh, startFunc, startCallback, isEndFunc, endCallback))
}

func PostBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) {
	rh.POST(rh.RelativePath, bizHandlerBytes(rh, startFunc, startCallback, isEndFunc, endCallback))
}

func bizHandlerJson[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) gin.HandlerFunc {
	return bizHandler(rh, startFunc, startCallback, isEndFunc, endCallback, func(ctx *dgctx.DgContext, conn *websocket.Conn, forwardConn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[T], error) {
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

		return &WebSocketMessage[T]{Connection: conn, ForwardConn: forwardConn, MessageType: mt, MessageData: req}, nil
	})
}

func bizHandlerBytes(rh *wrapper.RequestHolder[WebSocketMessage[[]byte], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc) gin.HandlerFunc {
	return bizHandler(rh, startFunc, startCallback, isEndFunc, endCallback, func(ctx *dgctx.DgContext, conn *websocket.Conn, forwardConn *websocket.Conn, mt int, data []byte) (*WebSocketMessage[[]byte], error) {
		dglogger.Infof(ctx, "server receive msg size: %d", len(data))
		return &WebSocketMessage[[]byte]{Connection: conn, ForwardConn: forwardConn, MessageType: mt, MessageData: &data}, nil
	})
}

func bizHandler[T any](rh *wrapper.RequestHolder[WebSocketMessage[T], error], startFunc StartFunc, startCallback ForwardCallbackFunc, isEndFunc IsEndFunc, endCallback ForwardCallbackFunc, buildWsMessage buildWsMessageFunc[T]) gin.HandlerFunc {
	return func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				c.AbortWithStatusJSON(http.StatusOK, result.FailByDgError[dgerr.DgError](dgerr.SYSTEM_BUSY))
				return
			}
			defer semaphore.Release()
		}
		ctx := utils.GetDgContext(c)

		// 服务升级，对于来到的http连接进行服务升级，升级到ws
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			dglogger.Errorf(ctx, "upgrade error: %v", err)
			return
		}

		defer func(wc *websocket.Conn) {
			err := wc.Close()
			if err != nil {
				dglogger.Errorf(ctx, "close websocket conn error: %v", err)
			}
		}(conn)

		forwardConn, err := startFunc(ctx, conn)
		if err != nil {
			dglogger.Errorf(ctx, "start websocket error: %v", err)
			return
		}
		if forwardConn != nil {
			defer func(wc *websocket.Conn) {
				err := wc.Close()
				if err != nil {
					dglogger.Errorf(ctx, "close forward websocket conn error: %v", err)
				}
			}(forwardConn)

			if startCallback != nil {
				err := startCallback(ctx, forwardConn)
				if err != nil {
					dglogger.Errorf(ctx, "start callback error: %v", err)
				}
			}
		}

		for {
			mt, message, err := conn.ReadMessage()
			if isEndFunc == nil {
				isEndFunc = DefaultIsEndFunc
			}
			if isEndFunc(mt, message) {
				ctx.SetExtraKeyValue(endedKey, true)
				dglogger.Infof(ctx, "server receive close message, error: %v", err)
				if endCallback != nil && forwardConn != nil {
					err := endCallback(ctx, forwardConn)
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

			wsm, err := buildWsMessage(ctx, conn, forwardConn, mt, message)
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
