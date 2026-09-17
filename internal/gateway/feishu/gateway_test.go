package feishu

import (
	"context"
	"testing"
	"time"

	channeltypes "github.com/larksuite/oapi-sdk-go/v3/channel/types"
)

type stubAgent struct{}

func (stubAgent) Chat(context.Context, string, string) (string, error) {
	return "ok", nil
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		agent  Agent
	}{
		{name: "missing app id", config: Config{AppSecret: "secret"}, agent: stubAgent{}},
		{name: "missing app secret", config: Config{AppID: "app"}, agent: stubAgent{}},
		{name: "missing agent", config: Config{AppID: "app", AppSecret: "secret"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.config, tt.agent); err == nil {
				t.Fatal("New() error = nil, want validation error")
			}
		})
	}
}

func TestNew(t *testing.T) {
	gateway, err := New(Config{AppID: "app", AppSecret: "secret"}, stubAgent{})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	if gateway == nil {
		t.Fatal("New() returned nil gateway")
	}
}

func TestNewWSClientConfiguresEventDispatcher(t *testing.T) {
	wsClient := newWSClient(Config{AppID: "app", AppSecret: "secret"})
	if wsClient.EventHandler() == nil {
		t.Fatal("newWSClient() EventHandler() = nil, want dispatcher")
	}
}

func TestRunChannelStopsOnContextCancel(t *testing.T) {
	ch := newBlockingChannel()
	gateway := &Gateway{agent: stubAgent{}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- gateway.runChannel(ctx, ch)
	}()

	<-ch.started
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runChannel() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runChannel() did not return after context cancellation")
	}

	select {
	case <-ch.stopped:
	default:
		t.Fatal("runChannel() did not stop channel after context cancellation")
	}
	close(ch.release)
}

type blockingChannel struct {
	started chan struct{}
	stopped chan struct{}
	release chan struct{}
}

func newBlockingChannel() *blockingChannel {
	return &blockingChannel{
		started: make(chan struct{}),
		stopped: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (c *blockingChannel) Send(context.Context, *channeltypes.SendInput) (*channeltypes.SendResult, error) {
	return nil, nil
}

func (c *blockingChannel) OnMessage(func(context.Context, *channeltypes.NormalizedMessage) error) {}

func (c *blockingChannel) OnReaction(func(context.Context, *channeltypes.ReactionEvent) error) {}

func (c *blockingChannel) OnComment(func(context.Context, *channeltypes.CommentEvent) error) {}

func (c *blockingChannel) OnBotAdded(func(context.Context, *channeltypes.BotAddedEvent) error) {}

func (c *blockingChannel) OnCardAction(func(context.Context, *channeltypes.CardActionEvent) error) {}

func (c *blockingChannel) OnReject(func(context.Context, *channeltypes.RejectEvent) error) {}

func (c *blockingChannel) DownloadFile(context.Context, string, string) ([]byte, error) {
	return nil, nil
}

func (c *blockingChannel) OnReady(func()) {}

func (c *blockingChannel) OnError(func(error)) {}

func (c *blockingChannel) OnReconnecting(func()) {}

func (c *blockingChannel) OnReconnected(func()) {}

func (c *blockingChannel) OnDisconnected(func()) {}

func (c *blockingChannel) Start(context.Context) error {
	close(c.started)
	<-c.release
	return nil
}

func (c *blockingChannel) Stream(context.Context, *channeltypes.SendInput) (channeltypes.StreamController, error) {
	return nil, nil
}

func (c *blockingChannel) UpdatePolicy(channeltypes.PolicyConfig) {}

func (c *blockingChannel) GetPolicy() channeltypes.PolicyConfig {
	return channeltypes.PolicyConfig{}
}

func (c *blockingChannel) GetBotIdentity(context.Context) *channeltypes.BotIdentity {
	return nil
}

func (c *blockingChannel) Stop(context.Context) error {
	close(c.stopped)
	return nil
}
