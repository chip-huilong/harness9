package feishu

import (
	"testing"

	"github.com/harness9/internal/engine"
	"github.com/harness9/internal/hooks"
)

func TestApproveGatewayRequestAllowsToolCall(t *testing.T) {
	respCh := make(chan hooks.ApprovalResponse, 1)

	approveGatewayRequest(engine.ApprovalRequest{ResponseCh: respCh})

	resp := <-respCh
	if !resp.Approved {
		t.Fatal("approveGatewayRequest() denied tool call, want approved")
	}
}
