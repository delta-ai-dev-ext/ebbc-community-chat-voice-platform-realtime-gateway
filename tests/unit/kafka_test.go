package unit

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
	"realtime_gateway/internal/gateway"
	"realtime_gateway/internal/models"
)

// fakeClusterClient is an in-process Kafka metadata stub.
type fakeClusterClient struct {
	partitions map[string][]int32
	err        error
	closed     atomic.Int32
}

// Partitions returns the configured partition ids.
func (c *fakeClusterClient) Partitions(topic string) ([]int32, error) {
	if c.err != nil {
		return nil, c.err
	}
	return c.partitions[topic], nil
}

// Close records the shutdown.
func (c *fakeClusterClient) Close() error {
	c.closed.Add(1)
	return nil
}

// fakePartitionConsumer is one in-process partition stream.
type fakePartitionConsumer struct {
	messages  chan *sarama.ConsumerMessage
	errs      chan *sarama.ConsumerError
	highWater atomic.Int64
	closed    atomic.Int32
}

// newFakePartitionConsumer builds a partition stream with the given buffer.
func newFakePartitionConsumer(buffer int) *fakePartitionConsumer {
	return &fakePartitionConsumer{
		messages: make(chan *sarama.ConsumerMessage, buffer),
		errs:     make(chan *sarama.ConsumerError, buffer),
	}
}

// Messages returns the message channel.
func (p *fakePartitionConsumer) Messages() <-chan *sarama.ConsumerMessage { return p.messages }

// Errors returns the error channel.
func (p *fakePartitionConsumer) Errors() <-chan *sarama.ConsumerError { return p.errs }

// HighWaterMarkOffset returns the configured high-water mark.
func (p *fakePartitionConsumer) HighWaterMarkOffset() int64 { return p.highWater.Load() }

// Close stops the partition stream.
func (p *fakePartitionConsumer) Close() error {
	if p.closed.Add(1) == 1 {
		close(p.messages)
		close(p.errs)
	}
	return nil
}

// fakeConsumerFactory assigns partitions and records the requested offsets.
type fakeConsumerFactory struct {
	mu        sync.Mutex
	consumers map[string][]*fakePartitionConsumer
	offsets   []int64
	err       error
}

// ConsumePartition registers one fake partition consumer.
func (f *fakeConsumerFactory) ConsumePartition(topic string, partition int32, offset int64) (gateway.PartitionConsumer, error) {
	if f.err != nil {
		return nil, f.err
	}
	f.mu.Lock()
	defer f.mu.Unlock()

	consumer := newFakePartitionConsumer(4)
	f.consumers[topic] = append(f.consumers[topic], consumer)
	f.offsets = append(f.offsets, offset)
	return consumer, nil
}

// topicConsumers returns the consumers created for a topic.
func (f *fakeConsumerFactory) topicConsumers(topic string) []*fakePartitionConsumer {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]*fakePartitionConsumer(nil), f.consumers[topic]...)
}

// requestedOffsets returns the offsets requested for ConsumePartition calls.
func (f *fakeConsumerFactory) requestedOffsets() []int64 {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]int64(nil), f.offsets...)
}

// feedFixture wires a Kafka feed over fakes plus the pipeline behind it.
type feedFixture struct {
	feed    *gateway.KafkaFeed
	factory *fakeConsumerFactory
	hub     *gateway.Hub
	metrics *gateway.Metrics
	cancel  context.CancelFunc
	done    chan struct{}
	clients atomic.Int32

	mu     sync.Mutex
	client *fakeClusterClient
}

// setClient records the client used by the current attach attempt.
func (f *feedFixture) setClient(client *fakeClusterClient) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.client = client
}

// getClient returns the client of the current (or last) attach attempt.
func (f *feedFixture) getClient() *fakeClusterClient {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.client
}

// newFeedFixture starts a feed over the fake broker.
func newFeedFixture(t *testing.T, clientFunc func() (*fakeClusterClient, error)) *feedFixture {
	t.Helper()

	metrics := newTestMetrics(t)
	hub := gateway.NewHub(metrics, gateway.HubOptions{})
	pipeline := gateway.NewPipeline(hub, contracts.AllTopics, time.Minute, zap.NewNop(), metrics)
	factory := &fakeConsumerFactory{consumers: map[string][]*fakePartitionConsumer{}}

	fixture := &feedFixture{hub: hub, metrics: metrics, factory: factory, done: make(chan struct{})}

	feed := gateway.NewKafkaFeed(gateway.FeedConfig{
		Brokers:      []string{"fake:9092"},
		Topics:       contracts.AllTopics,
		RetryBackoff: 5 * time.Millisecond,
		LagInterval:  5 * time.Millisecond,
	}, pipeline, zap.NewNop(), gateway.FeedDeps{
		NewClient: func(gateway.FeedConfig) (gateway.ClusterClient, error) {
			fixture.clients.Add(1)
			client, err := clientFunc()
			if err != nil {
				return nil, err
			}
			fixture.setClient(client)
			return client, nil
		},
		NewConsumer: func(gateway.ClusterClient) (gateway.ConsumerFactory, error) {
			return factory, nil
		},
	}, metrics)
	fixture.feed = feed

	ctx, cancel := context.WithCancel(context.Background())
	fixture.cancel = cancel
	go func() {
		defer close(fixture.done)
		feed.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-fixture.done
	})
	return fixture
}

// defaultPartitions is the partition assignment of the fake broker.
func defaultPartitions() map[string][]int32 {
	partitions := make(map[string][]int32, len(contracts.AllTopics))
	for _, topic := range contracts.AllTopics {
		partitions[topic] = []int32{0, 1}
	}
	return partitions
}

func TestFeedAttachesEveryPartitionAtLatestOffset(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})

	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	offsets := fixture.factory.requestedOffsets()
	assert.Len(t, offsets, len(contracts.AllTopics)*2, "every partition of every topic is assigned")
	for _, offset := range offsets {
		assert.Equal(t, sarama.OffsetNewest, offset, "broadcast readers start at the newest offset")
	}
	for _, topic := range contracts.AllTopics {
		assert.Len(t, fixture.factory.topicConsumers(topic), 2, topic)
	}
}

func TestFeedDeliversConsumedEvents(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	community := models.CommunityScope(uuidAt(12))
	conn := newTestConnection(t, "c1", uuidAt(13), community)
	fixture.hub.Register(conn)

	consumers := fixture.factory.topicConsumers(contracts.TopicMessageEvents)
	require.NotEmpty(t, consumers)
	consumers[0].messages <- &sarama.ConsumerMessage{
		Topic:     contracts.TopicMessageEvents,
		Partition: 0,
		Offset:    7,
		Value:     rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), messagePayload()),
	}

	frame := awaitFrame(t, conn, 3*time.Second)
	assert.Equal(t, contracts.EventMessageChanged, frame.Type)
	assert.Equal(t, string(community), frame.Scope)
}

func TestFeedIgnoresConsumerErrors(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	community := models.CommunityScope(uuidAt(12))
	conn := newTestConnection(t, "c1", uuidAt(13), community)
	fixture.hub.Register(conn)

	consumers := fixture.factory.topicConsumers(contracts.TopicMessageEvents)
	consumers[0].errs <- &sarama.ConsumerError{Err: errors.New("temporary broker hiccup")}
	consumers[0].messages <- &sarama.ConsumerMessage{
		Value: rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), messagePayload()),
	}

	// A single consumer error must not detach the feed.
	assert.Equal(t, contracts.EventMessageChanged, awaitFrame(t, conn, 3*time.Second).Type)
	assert.True(t, fixture.feed.Attached())
}

func TestFeedDetachesWhenAPartitionClosesAndReattaches(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	consumers := fixture.factory.topicConsumers(contracts.TopicMessageEvents)
	require.NoError(t, consumers[0].Close())

	// The feed detaches (readiness 503) and retries the whole assignment.
	require.Eventually(t, func() bool { return fixture.clients.Load() >= 2 }, 3*time.Second, 5*time.Millisecond)
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)
}

func TestFeedRetriesWhileTheClusterIsUnavailable(t *testing.T) {
	t.Parallel()

	var attempts atomic.Int32
	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		if attempts.Add(1) < 3 {
			return nil, errors.New("no broker available")
		}
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})

	assert.False(t, fixture.feed.Attached(), "the feed starts detached")
	require.Eventually(t, fixture.feed.Attached, 5*time.Second, 5*time.Millisecond)
	assert.GreaterOrEqual(t, attempts.Load(), int32(3))
}

func TestFeedStaysDetachedOnPartitionMetadataFailure(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{err: errors.New("metadata unavailable")}, nil
	})

	require.Eventually(t, func() bool { return fixture.clients.Load() >= 2 }, 3*time.Second, 5*time.Millisecond)
	assert.False(t, fixture.feed.Attached())
	// The client is closed on every failed attempt.
	require.Eventually(t, func() bool {
		client := fixture.getClient()
		return client != nil && client.closed.Load() >= 1
	}, 3*time.Second, 5*time.Millisecond)
}

func TestFeedPublishesConsumerLag(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	consumers := fixture.factory.topicConsumers(contracts.TopicMessageEvents)
	require.NotEmpty(t, consumers)
	consumers[0].highWater.Store(10)
	consumers[0].messages <- &sarama.ConsumerMessage{
		Offset: 4,
		Value:  rawEvent(t, envelopeFor(contracts.EventMessageChanged, 1), messagePayload()),
	}

	// lag = high-water (10) - (last offset (4) + 1) = 5
	require.Eventually(t, func() bool {
		value := gaugeValue(t, fixture.metrics.KafkaLag.WithLabelValues(contracts.TopicMessageEvents, gateway.ConsumerGroupLabel))
		return value == 5
	}, 3*time.Second, 5*time.Millisecond)
}

func TestFeedStopsOnContextCancel(t *testing.T) {
	t.Parallel()

	fixture := newFeedFixture(t, func() (*fakeClusterClient, error) {
		return &fakeClusterClient{partitions: defaultPartitions()}, nil
	})
	require.Eventually(t, fixture.feed.Attached, 3*time.Second, 5*time.Millisecond)

	fixture.cancel()
	<-fixture.done

	assert.False(t, fixture.feed.Attached())
	client := fixture.getClient()
	require.NotNil(t, client)
	assert.Equal(t, int32(1), client.closed.Load())
}

func TestNewSaramaConfigIsABroadcastReader(t *testing.T) {
	t.Parallel()

	cfg := gateway.NewSaramaConfig(gateway.FeedConfig{
		Brokers:       []string{"kafka:9092"},
		Topics:        contracts.AllTopics,
		SASLMechanism: "PLAIN",
		SASLUser:      "user",
		SASLPassword:  "secret",
		TLSEnabled:    true,
	})

	assert.Equal(t, sarama.OffsetNewest, cfg.Consumer.Offsets.Initial)
	assert.False(t, cfg.Consumer.Offsets.AutoCommit.Enable, "offsets are never committed")
	assert.True(t, cfg.Consumer.Return.Errors)
	assert.True(t, cfg.Net.TLS.Enable)
	require.NotNil(t, cfg.Net.TLS.Config)
	assert.True(t, cfg.Net.SASL.Enable)
	assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypePlaintext), cfg.Net.SASL.Mechanism)
	assert.Equal(t, "user", cfg.Net.SASL.User)
	assert.Equal(t, sarama.SASLHandshakeV1, cfg.Net.SASL.Version)

	plain := gateway.NewSaramaConfig(gateway.FeedConfig{Brokers: []string{"kafka:9092"}})
	assert.False(t, plain.Net.TLS.Enable)
	assert.False(t, plain.Net.SASL.Enable)
}

func TestTopicListReturnsAllConsumedTopics(t *testing.T) {
	t.Parallel()

	topics := gateway.TopicList()
	assert.Equal(t, contracts.AllTopics, topics)

	// The returned slice is a copy: mutating it must not alter the contract.
	topics[0] = "mutated"
	assert.Equal(t, contracts.TopicMessageEvents, gateway.TopicList()[0])
}
