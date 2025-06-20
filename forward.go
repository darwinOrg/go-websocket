package dgws

import (
	dgctx "github.com/darwinOrg/go-common/context"
	dghttp "github.com/darwinOrg/go-httpclient"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/darwinOrg/go-web/utils"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"net/http"
	"sync/atomic"
)

func WebSocketForward(c *gin.Context, url string) {
	externalConn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if externalConn != nil {
		defer func() { _ = externalConn.Close() }()
	}

	ctx := utils.GetDgContext(c)
	if err != nil {
		dglogger.Errorf(ctx, "upgrader.Upgrade error: %v", err)
		return
	}

	wsHeader := http.Header{}
	dghttp.FillHeadersWithDgContext(ctx, wsHeader)
	internalConn, _, err := websocket.DefaultDialer.Dial(url, wsHeader)
	if internalConn != nil {
		defer func() { _ = internalConn.Close() }()
	}
	if err != nil {
		dglogger.Errorf(ctx, "dial internal server: %v", err)
		return
	}

	needClose := new(atomic.Bool)
	needClose.Store(false)

	go func() {
		syncWsMessage(ctx, internalConn, externalConn, needClose)
	}()

	syncWsMessage(ctx, externalConn, internalConn, needClose)
}

func syncWsMessage(ctx *dgctx.DgContext, sourceConn *websocket.Conn, destConn *websocket.Conn, needClose *atomic.Bool) {
	for {
		if needClose.Load() {
			break
		}

		mt, message, err := sourceConn.ReadMessage()
		_ = destConn.WriteMessage(mt, message)

		if mt == websocket.CloseMessage || mt == -1 {
			dglogger.Infof(ctx, "received close message, error: %v", err)
			needClose.Store(true)
			break
		}
	}
}
