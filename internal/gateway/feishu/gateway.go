// Package feishu 将飞书机器人消息桥接到 harness9 Agent。
package feishu

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"

	lark "github.com/larksuite/oapi-sdk-go/v3"
	"github.com/larksuite/oapi-sdk-go/v3/channel"
	channeltypes "github.com/larksuite/oapi-sdk-go/v3/channel/types"
	larkcore "github.com/larksuite/oapi-sdk-go/v3/core"
	"github.com/larksuite/oapi-sdk-go/v3/event/dispatcher"
	larkim "github.com/larksuite/oapi-sdk-go/v3/service/im/v1"
	larkws "github.com/larksuite/oapi-sdk-go/v3/ws"

	"github.com/harness9/internal/logfmt"
)

// Agent 处理一条属于指定会话的用户消息。
type Agent interface {
	Chat(ctx context.Context, conversationID, prompt string) (string, error)
}

// Config 描述飞书 Gateway 的启动配置。
type Config struct {
	AppID     string
	AppSecret string
}

// Gateway 使用飞书长连接接收消息并回复。
type Gateway struct {
	config Config
	agent  Agent
}

// New 创建飞书 Gateway。
func New(config Config, agent Agent) (*Gateway, error) {
	if strings.TrimSpace(config.AppID) == "" {
		return nil, errors.New("FEISHU_APP_ID 未配置")
	}
	if strings.TrimSpace(config.AppSecret) == "" {
		return nil, errors.New("FEISHU_APP_SECRET 未配置")
	}
	if agent == nil {
		return nil, errors.New("agent 不能为空")
	}
	return &Gateway{config: config, agent: agent}, nil
}

func newWSClient(config Config) *larkws.Client {
	eventDispatcher := dispatcher.NewEventDispatcher("", "")
	// 注册 message_read 空处理器，避免 SDK 因缺少 handler 打印错误日志
	eventDispatcher.OnP2MessageReadV1(func(_ context.Context, _ *larkim.P2MessageReadV1) error {
		return nil
	})
	return larkws.NewClient(
		config.AppID,
		config.AppSecret,
		larkws.WithLogLevel(larkcore.LogLevelInfo),
		larkws.WithEventHandler(eventDispatcher),
	)
}

// Run 启动长连接并阻塞，直到 ctx 被取消或连接启动失败。
func (g *Gateway) Run(ctx context.Context) error {
	client := lark.NewClient(g.config.AppID, g.config.AppSecret, lark.WithLogLevel(larkcore.LogLevelInfo))
	wsClient := newWSClient(g.config)
	ch := channel.NewChannel(client, wsClient)
	return g.runChannel(ctx, ch)
}

func (g *Gateway) runChannel(ctx context.Context, ch channeltypes.Channel) error {
	var tasks sync.WaitGroup

	ch.OnReady(func() {
		log.Print(logfmt.FormatMsg("gateway-feishu", "飞书长连接已就绪"))
	})
	ch.OnError(func(err error) {
		log.Print(logfmt.FormatMsg("gateway-feishu", fmt.Sprintf("飞书连接错误: %v", err)))
	})
	ch.OnMessage(func(_ context.Context, msg *channeltypes.NormalizedMessage) error {
		prompt := strings.TrimSpace(msg.Content)
		if prompt == "" {
			return nil
		}
		tasks.Add(1)
		go func() {
			defer tasks.Done()
			reply, err := g.agent.Chat(ctx, msg.ChatID, prompt)
			if err != nil {
				log.Print(logfmt.FormatMsg("gateway-feishu", fmt.Sprintf("Agent 执行失败: %v", err)))
				reply = "处理消息时发生错误，请稍后重试。"
			}
			if strings.TrimSpace(reply) == "" {
				reply = "Agent 已完成处理，但没有生成文本回复。"
			}
			if _, err := ch.Send(ctx, &channeltypes.SendInput{
				ChatID:         msg.ChatID,
				ReplyMessageID: msg.MessageID,
				Markdown:       reply,
			}); err != nil {
				log.Print(logfmt.FormatMsg("gateway-feishu", fmt.Sprintf("回复飞书消息失败: %v", err)))
			}
		}()
		return nil
	})

	startErrCh := make(chan error, 1)
	go func() {
		startErrCh <- ch.Start(ctx)
	}()

	select {
	case err := <-startErrCh:
		if err == nil {
			return nil
		}
		return fmt.Errorf("启动飞书长连接: %w", err)
	case <-ctx.Done():
	}
	if err := ch.Stop(context.Background()); err != nil {
		log.Print(logfmt.FormatMsg("gateway-feishu", fmt.Sprintf("停止飞书长连接失败: %v", err)))
	}
	tasks.Wait()
	return nil
}
