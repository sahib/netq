package server

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/sahib/timeq"
	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
)

const DefaultForkName = "default"

type TopicSpec string

func (t TopicSpec) Validate() error {
	// TODO: Figure out rules. Include fork names via slash?
	return nil
}

// Parse returns the topic name and the fork name. The result
// only makes sense if Validate() did not return an error.
func (t TopicSpec) Parse() (string, timeq.ForkName) {
	split := strings.SplitN(string(t), "/", 2)
	if len(split) != 2 {
		return split[0], DefaultForkName
	}

	topicName := split[0]
	forkName := timeq.ForkName(split[1])
	return topicName, forkName
}

func (t TopicSpec) ForkName() timeq.ForkName {
	_, forkName := t.Parse()
	return forkName
}

func (t TopicSpec) TopicName() string {
	topicName, _ := t.Parse()
	return topicName
}

////////////////

type Topic struct {
	broadcaster   chan bool
	boradcasterMu sync.Mutex
	waiting       *timeq.Queue
	unacked       *timeq.Queue
}

type timeqLogger struct {
}

func (tl timeqLogger) Printf(fmtSpec string, args ...any) {
	slog.Info(fmt.Sprintf("timeq: "+fmtSpec, args...))
}

type TopicOptions struct {
	// TODO: Option to limit size of unacked queue.
	//       (akin to "max unacked")
	TimeqOpts timeq.Options
}

func DefaultTopicOptions() TopicOptions {
	timeqOpts := timeq.DefaultOptions()
	timeqOpts.BucketFunc = timeq.ShiftBucketFunc(37)
	timeqOpts.ErrorMode = bucket.ErrorModeContinue
	timeqOpts.MaxParallelOpenBuckets = 4
	timeqOpts.Logger = timeqLogger{}

	return TopicOptions{
		TimeqOpts: timeqOpts,
	}
}

func NewTopic(dir string, opts TopicOptions) (*Topic, error) {
	waitingDir := filepath.Join(dir, "waiting")
	unackedDir := filepath.Join(dir, "unacked")

	waiting, err := timeq.Open(waitingDir, opts.TimeqOpts)
	if err != nil {
		return nil, err
	}

	unacked, err := timeq.Open(unackedDir, opts.TimeqOpts)
	if err != nil {
		return nil, err
	}

	return &Topic{
		broadcaster: make(chan bool),
		waiting:     waiting,
		unacked:     unacked,
	}, nil
}

func (t *Topic) wakeup() { // put on a little makeup
	t.boradcasterMu.Lock()
	defer t.boradcasterMu.Unlock()

	close(t.broadcaster)
	t.broadcaster = make(chan bool)
}

func (t *Topic) Push(items timeq.Items) error {
	return t.waiting.Push(items)
}

func (t *Topic) Close() error {
	return errors.Join(
		t.waiting.Close(),
		t.unacked.Close(),
	)
}

type TopicFork struct {
	t         *Topic
	wconsumer timeq.Consumer
	uconsumer timeq.Consumer
}

// TODO: Is deferring the error really a good idea?
func (t *Topic) Fork(name timeq.ForkName) (*TopicFork, error) {
	var wconsumer timeq.Consumer = t.waiting
	var uconsumer timeq.Consumer = t.unacked

	if name != DefaultForkName {
		// If the fork already exist, no extra work is done
		// beside some basic checking.

		wfork, err := t.waiting.Fork(name)
		if err != nil {
			return nil, err
		}

		ufork, err := t.unacked.Fork(name)
		if err != nil {
			return nil, err
		}

		wconsumer = wfork
		uconsumer = ufork
	}

	return &TopicFork{
		t:         t,
		wconsumer: wconsumer,
		uconsumer: uconsumer,
	}, nil
}

func (tf *TopicFork) Pop(n int, dst timeq.Items, maxWait time.Duration, fn timeq.ReadFn) error {
	maxWaitTimer := time.NewTimer(maxWait)
	for {
		isEmpty := true

		// NOTE: This is probably not obvious, so some explanation here:
		// Move() will move the data from the waiting queue (of this specific fork)
		// to the unacked queue (to the specific fork there).

		err := tf.wconsumer.Move(n, dst, tf.t.unacked, func(items item.Items) error {
			if len(items) == 0 {
				return nil
			}

			isEmpty = false
			return fn(items)
		})

		if err != nil {
			return err
		}

		if !isEmpty {
			// job done!
			return nil
		}

		// Queue is currently empty. We should  block until
		// it is not empty anymore (at some point at least)
		tf.t.boradcasterMu.Lock()
		ch := tf.t.broadcaster
		tf.t.boradcasterMu.Unlock()

		select {
		case <-ch:
			// something happened on the writing end,
			// we should go and pop from the queue again!
		case <-maxWaitTimer.C:
			// we exceed the maximum wait time and had nothing
			// to return.
			return nil
		}
	}

	return nil
}

func (tf *TopicFork) Ack(key timeq.Key) (int, error) {
	if key != math.MaxInt64 {
		// This makes DeleteLowerThan() to DeleteIncluding()
		key++
	}

	defer tf.t.wakeup()
	waitingDeleted, err := tf.wconsumer.DeleteLowerThan(key)
	if err != nil {
		return waitingDeleted, err
	}

	unackedDeleted, err := tf.uconsumer.DeleteLowerThan(key)
	if err != nil {
		return waitingDeleted + unackedDeleted, err
	}

	return waitingDeleted + unackedDeleted, nil
}

func (tf *TopicFork) Restart() (int, error) {
	defer tf.t.wakeup()
	return tf.wconsumer.Shovel(tf.t.unacked)
}

// TODO:
// IDEAS:
//
// * SUPPLY PRIORITY FROM CLIENT SIDE (by default time.Now().UnixNano())
// * PASS topic/fork ON SUB - CREATE topic + fork if needed.
// * HAVE MAXIMUM SIZE OF THE UNACKED QUEUE PER TOPIC (can't have it per fork...)
// * MAX-SIZE for each topic and general max-size

////////////////

type topicRef struct {
	RefCount int
	Topic    *Topic
}

// Topics is a registry for several forks
type Topics struct {
	mu     sync.Mutex
	dir    string
	loaded map[TopicSpec]*topicRef
	opts   TopicOptions
}

func NewTopics(dir string, opts TopicOptions) *Topics {
	return &Topics{
		dir:    dir,
		opts:   opts,
		loaded: make(map[TopicSpec]*topicRef),
	}
}

func (tt *Topics) Ref(ts TopicSpec) (*Topic, error) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	if err := ts.Validate(); err != nil {
		return nil, err
	}

	topicName, _ := ts.Parse()
	ref, ok := tt.loaded[ts]
	if !ok {
		topicDir := filepath.Join(tt.dir, topicName)
		topic, err := NewTopic(topicDir, tt.opts)
		if err != nil {
			return nil, err
		}

		tt.loaded[ts] = &topicRef{
			RefCount: 0,
			Topic:    topic,
		}
	}

	ref.RefCount++
	return ref.Topic, nil
}

func (tt *Topics) Unref(ts TopicSpec) {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	ref, ok := tt.loaded[ts]
	if !ok {
		// programmer mistake.
		return
	}

	ref.RefCount--
	if ref.RefCount > 0 {
		return
	}

	// ref count dropped below zero, so make sure to cleanup:
	delete(tt.loaded, ts)
	if err := ref.Topic.Close(); err != nil {
		slog.Warn("failed to close queue", "topic", ts, "err", err)
	}
}

func (tt *Topics) Close() error {
	tt.mu.Lock()
	defer tt.mu.Unlock()

	var err error
	for _, ref := range tt.loaded {
		err = errors.Join(err, ref.Topic.Close())
	}

	return err
}
