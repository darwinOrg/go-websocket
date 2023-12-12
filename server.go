package dgws

import (
	"encoding/json"
	dgctx "github.com/darwinOrg/go-common/context"
	dgerr "github.com/darwinOrg/go-common/enums/error"
	"github.com/darwinOrg/go-common/result"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/darwinOrg/go-web/utils"
	"github.com/darwinOrg/go-web/wrapper"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/rolandhe/saber/gocc"
	"net/http"
)

type WebSocketMessage struct {
	Connection  *websocket.Conn
	MessageType int
	MessageData []byte
}

type GetBizIdHandler func(c *gin.Context) string
type StartFunc func(c *gin.Context, ctx *dgctx.DgContext, conn *websocket.Conn) error
type IsEndFunc func(mt int, data []byte) bool
type EndCallbackFunc func(ctx *dgctx.DgContext, conn *websocket.Conn) error
type WebSocketMessageCallback[T any] func(ctx *dgctx.DgContext, wsm *WebSocketMessage) error

const (
	ConnKey           = "WsConn"
	WsEndedKey        = "WsEnded"
	ForwardConnKey    = "ForwardConn"
	ForwardWsEndedKey = "WsForwardEnded"
)

func SetConn(ctx *dgctx.DgContext, conn *websocket.Conn) {
	ctx.SetExtraKeyValue(ConnKey, conn)
}

func GetConn(ctx *dgctx.DgContext) *websocket.Conn {
	conn := ctx.GetExtraValue(ConnKey)
	if conn == nil {
		return nil
	}

	return conn.(*websocket.Conn)
}

func SetWsEnded(ctx *dgctx.DgContext) {
	ctx.SetExtraKeyValue(WsEndedKey, true)
}

func IsWsEnded(ctx *dgctx.DgContext) bool {
	ended := ctx.GetExtraValue(WsEndedKey)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
}

func SetForwardConn(ctx *dgctx.DgContext, forwardMark string, conn *websocket.Conn) {
	ctx.SetExtraKeyValue(ForwardConnKey+forwardMark, conn)
}

func GetForwardConn(ctx *dgctx.DgContext, forwardMark string) *websocket.Conn {
	conn := ctx.GetExtraValue(ForwardConnKey + forwardMark)
	if conn == nil {
		return nil
	}

	return conn.(*websocket.Conn)
}

func SetForwardWsEnded(ctx *dgctx.DgContext, forwardMark string) {
	ctx.SetExtraKeyValue(ForwardWsEndedKey+forwardMark, true)
}

func UnsetForwardWsEnded(ctx *dgctx.DgContext, forwardMark string) {
	ctx.SetExtraKeyValue(ForwardWsEndedKey+forwardMark, false)
}

func IsForwardWsEnded(ctx *dgctx.DgContext, forwardMark string) bool {
	ended := ctx.GetExtraValue(ForwardWsEndedKey + forwardMark)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
}

func DefaultStartFunc(_ *gin.Context, _ *dgctx.DgContext, _ *websocket.Conn) error {
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

func GetBytes(rh *wrapper.RequestHolder[WebSocketMessage, error], bizKey string, getBizIdHandler GetBizIdHandler, startFunc StartFunc, isEndFunc IsEndFunc, endCallback EndCallbackFunc) {
	rh.GET(rh.RelativePath, func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				c.AbortWithStatusJSON(http.StatusOK, result.FailByDgError[dgerr.DgError](dgerr.SYSTEM_BUSY))
				return
			}
			defer semaphore.Release()
		}
		ctx := utils.GetDgContext(c)
		bizId := getBizIdHandler(c)

		// 服务升级，对于来到的http连接进行服务升级，升级到ws
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			dglogger.Errorf(ctx, "[%s: %s] upgrade error: %v", bizKey, bizId, err)
			return
		}
		SetConn(ctx, conn)
		defer conn.Close()

		if startFunc == nil {
			startFunc = DefaultStartFunc
		}
		err = startFunc(c, ctx, conn)
		if err != nil {
			dglogger.Errorf(ctx, "[%s: %s] start websocket error: %v", bizKey, bizId, err)
			WriteErrorResult(conn, err)
			return
		}

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
				dglogger.Infof(ctx, "[%s: %s] server receive close message, error: %v", bizKey, bizId, err)
				if endCallback != nil {
					err := endCallback(ctx, conn)
					if err != nil {
						dglogger.Errorf(ctx, "[%s: %s] end callback error: %v", bizKey, bizId, err)
					}
				}
				conn.WriteMessage(websocket.CloseMessage, message)
				break
			}

			if mt == websocket.PingMessage {
				dglogger.Infof(ctx, "[%s: %s] server receive ping message", bizKey, bizId)
				conn.WriteMessage(websocket.PongMessage, []byte("ok"))
				continue
			}

			if mt == websocket.PongMessage {
				continue
			}

			if err != nil {
				dglogger.Errorf(ctx, "[%s: %s] server read error: %v", bizKey, bizId, err)
				break
			}

			wsm := &WebSocketMessage{Connection: conn, MessageType: mt, MessageData: message}
			err = rh.BizHandler(c, ctx, wsm)
			if err != nil {
				dglogger.Errorf(ctx, "[%s: %s] biz handle message[%s] error: %v", message, bizKey, bizId, err)
			}
		}
	})
}

func WriteErrorResult(conn *websocket.Conn, err error) {
	rt := result.SimpleFail[string](err.Error())
	rtBytes, _ := json.Marshal(rt)
	conn.WriteMessage(websocket.TextMessage, rtBytes)
}

func WriteDgErrorResult(conn *websocket.Conn, err *dgerr.DgError) {
	rt := result.FailByError[*dgerr.DgError](err)
	rtBytes, _ := json.Marshal(rt)
	conn.WriteMessage(websocket.TextMessage, rtBytes)
}
