package feishu

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"github.com/harness9/internal/engine"
	"github.com/harness9/internal/hooks"
	"github.com/harness9/internal/memory"
)

const sessionSchema = `
CREATE TABLE IF NOT EXISTS gateway_sessions (
    gateway        TEXT NOT NULL,
    conversation_id TEXT NOT NULL,
    session_id     TEXT NOT NULL,
    PRIMARY KEY (gateway, conversation_id),
    FOREIGN KEY (session_id) REFERENCES sessions(id) ON DELETE CASCADE
)`

// SessionAgent 将飞书会话映射到持久化 Session。同一个 Engine 在切换 Session 时必须串行运行。
type SessionAgent struct {
	mu      sync.Mutex
	engine  *engine.AgentEngine
	manager *memory.Manager
}

// NewSessionAgent 创建具有飞书会话隔离能力的 Agent 适配器。
func NewSessionAgent(eng *engine.AgentEngine, manager *memory.Manager) (*SessionAgent, error) {
	if eng == nil || manager == nil {
		return nil, fmt.Errorf("engine 和 memory manager 不能为空")
	}
	if _, err := manager.DB().Exec(sessionSchema); err != nil {
		return nil, fmt.Errorf("初始化 gateway session schema: %w", err)
	}
	return &SessionAgent{engine: eng, manager: manager}, nil
}

// Chat 在对应飞书会话中运行 Agent，并收集可见文本回复。
func (a *SessionAgent) Chat(ctx context.Context, conversationID, prompt string) (string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	sess, err := a.session(ctx, conversationID)
	if err != nil {
		return "", err
	}
	a.engine.SetSession(sess)

	stream, err := a.engine.RunStream(ctx, prompt)
	if err != nil {
		return "", fmt.Errorf("启动 Agent: %w", err)
	}
	var reply strings.Builder
	for event := range stream {
		switch event.Type {
		case engine.EventActionDelta:
			if text, ok := event.Data.(string); ok {
				reply.WriteString(text)
			}
		case engine.EventApprovalRequired:
			approveGatewayRequest(event.Data)
		case engine.EventError:
			return "", fmt.Errorf("Agent 执行失败: %v", event.Data)
		}
	}
	return strings.TrimSpace(reply.String()), nil
}

func approveGatewayRequest(data interface{}) {
	if request, ok := data.(engine.ApprovalRequest); ok {
		request.ResponseCh <- hooks.ApprovalResponse{Approved: true}
	}
}

func (a *SessionAgent) session(ctx context.Context, conversationID string) (memory.Session, error) {
	var sessionID string
	err := a.manager.DB().QueryRowContext(ctx,
		`SELECT session_id FROM gateway_sessions WHERE gateway = ? AND conversation_id = ?`,
		"feishu", conversationID).Scan(&sessionID)
	if err == nil {
		return a.manager.OpenSession(ctx, sessionID)
	}
	if err != sql.ErrNoRows {
		return nil, fmt.Errorf("查询飞书会话映射: %w", err)
	}

	sess, err := a.manager.NewSession(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := a.manager.DB().ExecContext(ctx,
		`INSERT INTO gateway_sessions (gateway, conversation_id, session_id) VALUES (?, ?, ?)`,
		"feishu", conversationID, sess.SessionID()); err != nil {
		return nil, fmt.Errorf("保存飞书会话映射: %w", err)
	}
	return sess, nil
}
