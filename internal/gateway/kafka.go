package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.uber.org/zap"

	"realtime_gateway/internal/contracts"
)

// ConsumerGroupLabel is the constant `consumer_group` label of kafka_consumer_lag. The
// gateway is a broadcast reader: it belongs to no consumer group, so the label is fixed and
// low cardinality.
const ConsumerGroupLabel = "broadcast"

// Feed tuning defaults.
const (
	defaultFeedRetryBackoff = 2 * time.Second
	defaultFeedLagInterval  = time.Second
)

// ErrPartitionClosed signals that a partition consumer stopped delivering, which detaches
// the feed and flips readiness to 503 until it reattaches.
var ErrPartitionClosed = errors.New("partition consumer closed")

// ClusterClient is the Kafka metadata surface the feed needs. sarama.Client satisfies it.
type ClusterClient interface {
	// Partitions returns every partition id of a topic (manual assignment, no group).
	Partitions(topic string) ([]int32, error)
	// Close releases the client.
	Close() error
}

// PartitionConsumer is one manually assigned partition reader. sarama.PartitionConsumer
// satisfies it.
type PartitionConsumer interface {
	Messages() <-chan *sarama.ConsumerMessage
	Errors() <-chan *sarama.ConsumerError
	HighWaterMarkOffset() int64
	Close() error
}

// ConsumerFactory creates partition consumers from a cluster client.
type ConsumerFactory interface {
	ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error)
}

// FeedConfig is the Kafka configuration of the broadcast reader.
type FeedConfig struct {
	// Brokers is the bootstrap broker list.
	Brokers []string
	// Topics are the consumed topics (all five platform topics in production).
	Topics []string
	// SASLMechanism, SASLUser and SASLPassword enable SASL when the mechanism is set.
	SASLMechanism string
	SASLUser      string
	SASLPassword  string
	// TLSEnabled turns on TLS for broker connections.
	TLSEnabled bool
	// RetryBackoff is the wait between re-attach attempts after a detach.
	RetryBackoff time.Duration
	// LagInterval is how often kafka_consumer_lag is recomputed.
	LagInterval time.Duration
}

// FeedDeps allows tests to replace the broker connection with in-process fakes.
type FeedDeps struct {
	// NewClient creates the Kafka client (defaults to sarama.NewClient).
	NewClient func(FeedConfig) (ClusterClient, error)
	// NewConsumer creates the partition-consumer factory (defaults to a sarama consumer).
	NewConsumer func(ClusterClient) (ConsumerFactory, error)
}

// KafkaFeed is the broadcast consumer: on every replica it assigns itself every partition of
// every topic, starts at the latest offset, never commits and never joins a consumer group.
// A detach flips Attached() to false so /readyz reports 503 and the replica stops taking new
// connections until the feed is reattached.
type KafkaFeed struct {
	cfg      FeedConfig
	pipeline *Pipeline
	logger   *zap.Logger
	metrics  *Metrics
	deps     FeedDeps

	mu       sync.RWMutex
	attached map[string]int // topic → live partition consumers
	client   ClusterClient
	parts    []PartitionConsumer
	progress map[string]map[int32]*partitionProgress
	stopped  bool
}

// partitionProgress tracks the last delivered offset of one partition so lag can be derived
// from the partition's high-water mark without committing offsets.
type partitionProgress struct {
	mu       sync.Mutex
	last     int64
	highMark int64
}

// NewKafkaFeed builds the broadcast consumer.
func NewKafkaFeed(cfg FeedConfig, pipeline *Pipeline, logger *zap.Logger, deps FeedDeps, metrics *Metrics) *KafkaFeed {
	if logger == nil {
		logger = zap.NewNop()
	}
	if cfg.RetryBackoff <= 0 {
		cfg.RetryBackoff = defaultFeedRetryBackoff
	}
	if cfg.LagInterval <= 0 {
		cfg.LagInterval = defaultFeedLagInterval
	}
	if deps.NewClient == nil {
		deps.NewClient = defaultNewClient
	}
	if deps.NewConsumer == nil {
		deps.NewConsumer = defaultNewConsumer
	}
	return &KafkaFeed{
		cfg:      cfg,
		pipeline: pipeline,
		logger:   logger,
		metrics:  metrics,
		deps:     deps,
		attached: make(map[string]int, len(cfg.Topics)),
		progress: make(map[string]map[int32]*partitionProgress, len(cfg.Topics)),
	}
}

// Attached reports whether every configured topic has at least one live partition consumer
// on this replica. /readyz is 200 only while this holds.
func (f *KafkaFeed) Attached() bool {
	f.mu.RLock()
	defer f.mu.RUnlock()

	if f.stopped {
		return false
	}
	for _, topic := range f.cfg.Topics {
		if f.attached[topic] < 1 {
			return false
		}
	}
	return true
}

// Run consumes until ctx is cancelled. Broker outages keep the feed detached and are retried
// with a fixed backoff; they never terminate the process.
func (f *KafkaFeed) Run(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			f.markStopped()
			return
		}
		err := f.runOnce(ctx)
		f.clearAttached()
		if ctx.Err() != nil {
			f.markStopped()
			return
		}
		f.logger.Error("event feed detached",
			zap.Strings("topics", f.cfg.Topics),
			zap.Error(err),
		)

		select {
		case <-ctx.Done():
			f.markStopped()
			return
		case <-time.After(f.cfg.RetryBackoff):
		}
	}
}

// runOnce attaches every partition of every topic and blocks until the feed fails or ctx is
// cancelled, then releases all Kafka resources.
func (f *KafkaFeed) runOnce(ctx context.Context) error {
	client, err := f.deps.NewClient(f.cfg)
	if err != nil {
		return fmt.Errorf("connect kafka cluster: %w", err)
	}
	consumer, err := f.deps.NewConsumer(client)
	if err != nil {
		_ = client.Close()
		return fmt.Errorf("create kafka consumer: %w", err)
	}

	f.mu.Lock()
	f.client = client
	f.parts = nil
	f.progress = make(map[string]map[int32]*partitionProgress, len(f.cfg.Topics))
	f.mu.Unlock()

	fatal := make(chan error, 1)
	var wg sync.WaitGroup

	if err := f.attachAll(consumer, &wg, fatal); err != nil {
		f.release(client, &wg)
		return err
	}

	samplerDone := make(chan struct{})
	go f.sampleLag(ctx, samplerDone)

	var runErr error
	select {
	case <-ctx.Done():
		runErr = ctx.Err()
	case runErr = <-fatal:
	}
	close(samplerDone)
	f.release(client, &wg)
	return runErr
}

// attachAll assigns every partition of every topic at the latest offset.
func (f *KafkaFeed) attachAll(consumer ConsumerFactory, wg *sync.WaitGroup, fatal chan<- error) error {
	for _, topic := range f.cfg.Topics {
		partitions, err := f.partitionsOf(topic)
		if err != nil {
			return fmt.Errorf("list partitions of %s: %w", topic, err)
		}
		for _, partition := range partitions {
			// Broadcast reader: start at the newest offset, never replay history.
			pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
			if err != nil {
				return fmt.Errorf("assign %s/%d: %w", topic, partition, err)
			}
			progress := &partitionProgress{}
			f.track(topic, partition, pc, progress)

			wg.Add(1)
			go func(topic string, partition int32, pc PartitionConsumer, progress *partitionProgress) {
				defer wg.Done()
				// consumePartition only returns on a fatal detach: escalate it so runOnce
				// reattaches (readiness flips to 503 meanwhile).
				err := f.consumePartition(topic, partition, pc, progress)
				select {
				case fatal <- err:
				default:
				}
			}(topic, partition, pc, progress)
		}
	}
	return nil
}

// partitionsOf lists the partitions of a topic from the cluster client.
func (f *KafkaFeed) partitionsOf(topic string) ([]int32, error) {
	f.mu.RLock()
	client := f.client
	f.mu.RUnlock()

	if client == nil {
		return nil, errors.New("kafka client is not attached")
	}
	return client.Partitions(topic)
}

// track remembers a live partition consumer.
func (f *KafkaFeed) track(topic string, partition int32, pc PartitionConsumer, progress *partitionProgress) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.parts = append(f.parts, pc)
	f.attached[topic]++
	if f.progress[topic] == nil {
		f.progress[topic] = make(map[int32]*partitionProgress)
	}
	f.progress[topic][partition] = progress
}

// consumePartition decodes and dispatches messages of one partition until it closes.
func (f *KafkaFeed) consumePartition(topic string, partition int32, pc PartitionConsumer, progress *partitionProgress) error {
	for {
		select {
		case msg, ok := <-pc.Messages():
			if !ok {
				return fmt.Errorf("%w: %s/%d", ErrPartitionClosed, topic, partition)
			}
			if msg == nil {
				continue
			}
			progress.record(msg.Offset, pc.HighWaterMarkOffset())
			if f.pipeline != nil {
				f.pipeline.Handle(topic, msg.Value)
			}
		case consumerErr, ok := <-pc.Errors():
			if !ok {
				return fmt.Errorf("%w: %s/%d", ErrPartitionClosed, topic, partition)
			}
			if consumerErr != nil {
				// sarama retries internally; a single error must not kill the consumer.
				f.logger.Warn("kafka consumer error",
					zap.String("topic", topic),
					zap.Int32("partition", partition),
					zap.Error(consumerErr),
				)
			}
		}
	}
}

// sampleLag publishes kafka_consumer_lag per topic until done is closed.
func (f *KafkaFeed) sampleLag(ctx context.Context, done <-chan struct{}) {
	ticker := time.NewTicker(f.cfg.LagInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-done:
			return
		case <-ticker.C:
			f.publishLag()
		}
	}
}

// publishLag computes the per-topic lag from the tracked offsets and high-water marks.
func (f *KafkaFeed) publishLag() {
	f.mu.RLock()
	defer f.mu.RUnlock()

	for topic, partitions := range f.progress {
		var lag int64
		for _, progress := range partitions {
			lag += progress.lag()
		}
		f.metrics.SetLag(topic, ConsumerGroupLabel, float64(lag))
	}
}

// lag returns the number of messages this partition consumer is behind.
func (p *partitionProgress) lag() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.last <= 0 {
		return 0
	}
	if delta := p.highMark - (p.last + 1); delta > 0 {
		return delta
	}
	return 0
}

// record stores the last consumed offset and the current high-water mark.
func (p *partitionProgress) record(offset, highMark int64) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.last = offset
	p.highMark = highMark
}

// release closes every partition consumer and then the client, in that order.
func (f *KafkaFeed) release(client ClusterClient, wg *sync.WaitGroup) {
	f.mu.Lock()
	parts := f.parts
	f.parts = nil
	f.client = nil
	f.mu.Unlock()

	for _, pc := range parts {
		if err := pc.Close(); err != nil {
			f.logger.Debug("closing partition consumer", zap.Error(err))
		}
	}
	wg.Wait()

	if client != nil {
		if err := client.Close(); err != nil {
			f.logger.Debug("closing kafka client", zap.Error(err))
		}
	}
}

// clearAttached marks the feed detached (readiness 503) without stopping the process.
func (f *KafkaFeed) clearAttached() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.attached = make(map[string]int, len(f.cfg.Topics))
}

// markStopped records that Run has exited so Attached() stays false.
func (f *KafkaFeed) markStopped() {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = true
}

// NewSaramaConfig builds the sarama client configuration. The consumer is a broadcast
// reader: errors are surfaced, auto-commit is off and no consumer group is configured.
func NewSaramaConfig(cfg FeedConfig) *sarama.Config {
	saramaCfg := sarama.NewConfig()
	saramaCfg.ClientID = "realtime_gateway"
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Consumer.Return.Errors = true
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.Consumer.Offsets.AutoCommit.Enable = false
	saramaCfg.Consumer.IsolationLevel = sarama.ReadCommitted

	if cfg.TLSEnabled {
		saramaCfg.Net.TLS.Enable = true
		saramaCfg.Net.TLS.Config = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if cfg.SASLMechanism != "" {
		saramaCfg.Net.SASL.Enable = true
		saramaCfg.Net.SASL.Mechanism = sarama.SASLMechanism(cfg.SASLMechanism)
		saramaCfg.Net.SASL.User = cfg.SASLUser
		saramaCfg.Net.SASL.Password = cfg.SASLPassword
		saramaCfg.Net.SASL.Handshake = true
		if saramaCfg.Net.SASL.Mechanism == sarama.SASLTypePlaintext {
			saramaCfg.Net.SASL.Version = sarama.SASLHandshakeV1
		}
	}
	return saramaCfg
}

// defaultNewClient connects a sarama client to the configured brokers.
func defaultNewClient(cfg FeedConfig) (ClusterClient, error) {
	client, err := sarama.NewClient(cfg.Brokers, NewSaramaConfig(cfg))
	if err != nil {
		return nil, err
	}
	return client, nil
}

// defaultNewConsumer builds a sarama consumer (no consumer group) from the client.
func defaultNewConsumer(client ClusterClient) (ConsumerFactory, error) {
	saramaClient, ok := client.(sarama.Client)
	if !ok {
		return nil, errors.New("kafka client does not implement sarama.Client")
	}
	consumer, err := sarama.NewConsumerFromClient(saramaClient)
	if err != nil {
		return nil, err
	}
	return saramaConsumerFactory{consumer: consumer}, nil
}

// saramaConsumerFactory adapts sarama.Consumer to ConsumerFactory.
type saramaConsumerFactory struct {
	consumer sarama.Consumer
}

// ConsumePartition assigns one partition at the requested offset.
func (f saramaConsumerFactory) ConsumePartition(topic string, partition int32, offset int64) (PartitionConsumer, error) {
	pc, err := f.consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		return nil, err
	}
	return pc, nil
}

// TopicList returns the five consumed topics.
func TopicList() []string {
	return append([]string(nil), contracts.AllTopics...)
}
