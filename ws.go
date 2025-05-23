package dgws

import (
	"encoding/json"
	"errors"
	dgcoll "github.com/darwinOrg/go-common/collection"
	dgctx "github.com/darwinOrg/go-common/context"
	dgerr "github.com/darwinOrg/go-common/enums/error"
	"github.com/darwinOrg/go-common/result"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/darwinOrg/go-web/utils"
	"github.com/darwinOrg/go-web/wrapper"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/rolandhe/saber/gocc"
	"net"
	"net/http"
	"sync"
)

type GetBizIdHandler func(c *gin.Context) string
type StartHandler func(c *gin.Context, ctx *dgctx.DgContext, conn *websocket.Conn) error
type IsEndedHandler func(ctx *dgctx.DgContext, mt int, data []byte) bool
type EndCallbackHandler func(ctx *dgctx.DgContext, conn *websocket.Conn) error

type WebSocketMessage struct {
	Connection  *websocket.Conn
	MessageType int
	MessageData []byte
}

type WebSocketHandlerConfig struct {
	BizKey             string
	GetBizIdHandler    GetBizIdHandler
	StartHandler       StartHandler
	IsEndedHandler     IsEndedHandler
	EndCallbackHandler EndCallbackHandler
}

const (
	ConnKey                 = "WsConn"
	EndedKey                = "WsEnded"
	ForwardConnKey          = "WsForwardConn"
	ForwardConnTimestampKey = "WsForwardConnTimestamp"
	ForwardEndedKey         = "WsForwardEnded"
	WaitGroupKey            = "WsWaitGroup"
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
	ctx.SetExtraKeyValue(EndedKey, true)
}

func IsWsEnded(ctx *dgctx.DgContext) bool {
	ended := ctx.GetExtraValue(EndedKey)
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
	ctx.SetExtraKeyValue(ForwardEndedKey+forwardMark, true)
}

func UnsetForwardWsEnded(ctx *dgctx.DgContext, forwardMark string) {
	ctx.SetExtraKeyValue(ForwardEndedKey+forwardMark, false)
}

func IsForwardWsEnded(ctx *dgctx.DgContext, forwardMark string) bool {
	ended := ctx.GetExtraValue(ForwardEndedKey + forwardMark)
	if ended == nil {
		return false
	}

	e, ok := ended.(bool)
	return ok && e
}

func SetForwardConnTimestamp(ctx *dgctx.DgContext, forwardMark string, ts int64) {
	ctx.SetExtraKeyValue(ForwardConnTimestampKey+forwardMark, ts)
}

func GetForwardConnTimestamp(ctx *dgctx.DgContext, forwardMark string) int64 {
	ts := ctx.GetExtraValue(ForwardConnTimestampKey + forwardMark)
	if ts == nil {
		return 0
	}

	return ts.(int64)
}

func InitWaitGroup(ctx *dgctx.DgContext) {
	var waitGroup sync.WaitGroup
	SetWaitGroup(ctx, &waitGroup)
}

func SetWaitGroup(ctx *dgctx.DgContext, waitGroup *sync.WaitGroup) {
	ctx.SetExtraKeyValue(WaitGroupKey, waitGroup)
}

func GetWaitGroup(ctx *dgctx.DgContext) *sync.WaitGroup {
	waitGroup := ctx.GetExtraValue(WaitGroupKey)
	if waitGroup == nil {
		return nil
	}

	return waitGroup.(*sync.WaitGroup)
}

func IncrWaitGroup(ctx *dgctx.DgContext) {
	waitGroup := GetWaitGroup(ctx)
	if waitGroup != nil {
		waitGroup.Add(1)
	}
}

func DoneWaitGroup(ctx *dgctx.DgContext) {
	waitGroup := GetWaitGroup(ctx)
	if waitGroup != nil {
		waitGroup.Done()
	}
}

func WaitGroupAllDone(ctx *dgctx.DgContext) {
	waitGroup := GetWaitGroup(ctx)
	if waitGroup != nil {
		waitGroup.Wait()
	}
}

func DefaultStartHandler(_ *gin.Context, _ *dgctx.DgContext, _ *websocket.Conn) error {
	return nil
}

func DefaultIsEndHandler(_ *dgctx.DgContext, mt int, _ []byte) bool {
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

func Get(rh *wrapper.RequestHolder[WebSocketMessage, error], conf *WebSocketHandlerConfig) {
	bizHandler := func(c *gin.Context) {
		if semaphore != nil {
			if !semaphore.TryAcquire() {
				c.AbortWithStatusJSON(http.StatusOK, result.FailByDgError[dgerr.DgError](dgerr.SYSTEM_BUSY))
				return
			}
			defer semaphore.Release()
		}

		bizKey := conf.BizKey
		var bizId string
		if bizKey != "" && conf.GetBizIdHandler != nil {
			bizId = conf.GetBizIdHandler(c)
		}

		ctx := utils.GetDgContext(c)

		// 服务升级，对于来到的http连接进行服务升级，升级到ws
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			dglogger.Errorw(ctx, "upgrade error", "err", err, bizKey, bizId)
			return
		}
		SetConn(ctx, conn)
		defer func() {
			_ = conn.Close()
		}()

		if conf.StartHandler == nil {
			conf.StartHandler = DefaultStartHandler
		}
		err = conf.StartHandler(c, ctx, conn)
		if err != nil {
			dglogger.Errorw(ctx, "start websocket error", "err", err, bizKey, bizId)
			var dgError *dgerr.DgError
			switch {
			case errors.As(err, &dgError):
				WriteDgErrorResult(conn, err.(*dgerr.DgError))
			default:
				WriteErrorResult(conn, err)
			}
			return
		}

		if conf.IsEndedHandler == nil {
			conf.IsEndedHandler = DefaultIsEndHandler
		}

		for {
			if IsWsEnded(ctx) {
				break
			}

			mt, message, err := conn.ReadMessage()
			if err != nil {
				var ne net.Error
				switch {
				case errors.As(err, &ne):
					dglogger.Errorw(ctx, "server read message net error", bizKey, bizId)
					break
				}
			}

			if conf.IsEndedHandler(ctx, mt, message) {
				SetWsEnded(ctx)
				dglogger.Infow(ctx, "server receive close message error", "err", err, bizKey, bizId)
				if conf.EndCallbackHandler != nil {
					err := conf.EndCallbackHandler(ctx, conn)
					if err != nil {
						dglogger.Errorw(ctx, "end callback error", "err", err, bizKey, bizId)
					}
				}
				_ = conn.WriteMessage(websocket.CloseMessage, message)
				break
			}

			if err != nil {
				dglogger.Errorw(ctx, "server read error", "err", err, bizKey, bizId)
				break
			}

			if mt == websocket.PongMessage {
				continue
			}

			wsm := &WebSocketMessage{Connection: conn, MessageType: mt, MessageData: message}
			err = rh.BizHandler(c, ctx, wsm)
			if err != nil {
				dglogger.Errorw(ctx, "biz handle message error", "err", err, bizKey, bizId)
			}
		}
	}

	handlersChain := gin.HandlersChain{wrapper.LoginHandler(rh), wrapper.CheckProductHandler(rh), wrapper.CheckRolesHandler(rh), wrapper.CheckProfileHandler(), bizHandler}
	if len(rh.PreHandlersChain) > 0 {
		handlersChain = dgcoll.MergeToList(rh.PreHandlersChain, handlersChain)
	}

	rh.GET(rh.RelativePath, handlersChain...)
}

func WriteErrorResult(conn *websocket.Conn, err error) {
	rt := result.SimpleFail[string](err.Error())
	rtBytes, _ := json.Marshal(rt)
	_ = conn.WriteMessage(websocket.TextMessage, rtBytes)
}

func WriteDgErrorResult(conn *websocket.Conn, err *dgerr.DgError) {
	rt := result.FailByError[*dgerr.DgError](err)
	rtBytes, _ := json.Marshal(rt)
	_ = conn.WriteMessage(websocket.TextMessage, rtBytes)
}
