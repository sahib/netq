package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sahib/timeq"
	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/index"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
)

const DefaultForkName = "default"

type TopicSpec string

func (t TopicSpec) Validate() error {
	if len(t) == 0 {
		return errors.New("topic may not be empty")
	}

	// We just re-use the rules for fork-name here.
	split := strings.SplitN(string(t), "/", 2)
	if len(split) < 2 {
		return timeq.ForkName(t).Validate()
	}

	if err := timeq.ForkName(split[0]).Validate(); err != nil {
		return fmt.Errorf("topic name: %w", err)
	}

	if err := timeq.ForkName(split[1]).Validate(); err != nil {
		return fmt.Errorf("fork name: %w", err)
	}

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
	dir           string
	broadcaster   chan bool
	boradcasterMu sync.Mutex
	waiting       *timeq.Queue
	loadedUnacked map[string]*UnackedStore
}

type timeqLogger struct {
}

func (tl timeqLogger) Printf(fmtSpec string, args ...any) {
	slog.Info(fmt.Sprintf("timeq: "+fmtSpec, args...))
}

type TopicOptions struct {
	TimeqOpts timeq.Options
}

func (tp *TopicOptions) Validate() error {
	return tp.TimeqOpts.Validate()
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
	waiting, err := timeq.Open(waitingDir, opts.TimeqOpts)
	if err != nil {
		return nil, err
	}

	return &Topic{
		dir:           dir,
		broadcaster:   make(chan bool),
		waiting:       waiting,
		loadedUnacked: make(map[string]*UnackedStore, 1),
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
	return t.waiting.Close()
}

type TopicFork struct {
	t         *Topic
	wconsumer timeq.Consumer
	ustore    *UnackedStore
}

func (t *Topic) Fork(name timeq.ForkName) (*TopicFork, error) {
	var wconsumer timeq.Consumer = t.waiting
	if name != DefaultForkName {
		// If the fork already exist, no extra work is done
		// beside some basic checking.

		wfork, err := t.waiting.Fork(name)
		if err != nil {
			return nil, err
		}

		wconsumer = wfork
	}

	// NOTE: We should always use the same ustore for all forks that act on the
	// same topic. Otherwise we will get some nasty race conditions.
	unackedPath := filepath.Join(t.dir, "unacked", string(name))
	ustore, ok := t.loadedUnacked[unackedPath]
	if !ok {
		var err error
		ustore, err = LoadUnackedStore(unackedPath)
		if err != nil {
			return nil, err
		}

		t.loadedUnacked[unackedPath] = ustore
	}

	return &TopicFork{
		t:         t,
		wconsumer: wconsumer,
		ustore:    ustore,
	}, nil
}

func (tf *TopicFork) Pop(ctx context.Context, n int, dst timeq.Items, maxWait time.Duration, fn func(batchID uint64, items timeq.Items) error) error {
	maxWaitTimer := time.NewTimer(maxWait)
	for {
		isEmpty := true

		// NOTE: This is probably not obvious, so some explanation here:
		// Move() will move the data from the waiting queue (of this specific fork)
		// to the unacked queue (to the specific fork there).

		err := tf.wconsumer.Pop(n, dst, func(items item.Items) error {
			if len(items) == 0 {
				return nil
			}

			batchID, err := tf.ustore.Push(items)
			if err != nil {
				return err
			}

			isEmpty = false
			return fn(batchID, items)
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
		case <-ctx.Done():
			return nil
		case <-ch:
			// something happened on the writing end,
			// we should go and pop from the queue again!
		case <-maxWaitTimer.C:
			// we exceed the maximum wait time and had nothing
			// to return.
			return nil
		}
	}
}

func (tf *TopicFork) Ack(id uint64) (int, error) {
	return tf.ustore.Ack(id)
}

func (tf *TopicFork) Unacked() uint64 {
	return tf.ustore.Len()
}

func (tf *TopicFork) Restart() (int, error) {
	defer tf.t.wakeup()
	return tf.ustore.Clear(func(items timeq.Items) error {
		// Move the unacked messages back.
		return tf.t.waiting.Push(items)
	})
}

func (tf *TopicFork) Clear() (int, error) {
	defer tf.t.wakeup()

	waitingDeleted, err := tf.wconsumer.DeleteLowerThan(timeq.Key(math.MaxInt64))
	if err != nil {
		return 0, err
	}

	unackedDeleted, err := tf.ustore.Clear(nil)
	if err != nil {
		return waitingDeleted, err
	}

	return waitingDeleted + unackedDeleted, nil
}

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

		ref = &topicRef{
			RefCount: 0,
			Topic:    topic,
		}

		tt.loaded[ts] = ref
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

type UnackedStore struct {
	mu     sync.Mutex
	dir    string
	log    *vlog.Log
	idx    *index.Index
	idxLog *index.Writer
	seq    uint64
	seqFd  *os.File
}

func LoadUnackedStore(dir string) (*UnackedStore, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	seqPath := filepath.Join(dir, "seq.txt")
	seqData, err := os.ReadFile(seqPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	seqStr := string(bytes.TrimSpace(seqData))
	if len(seqStr) == 0 {
		seqStr = "0"
	}

	seq, err := strconv.ParseInt(seqStr, 10, 64)
	if err != nil {
		return nil, err
	}

	seqFd, err := os.OpenFile(seqPath, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}

	datLog := filepath.Join(dir, "dat.log")
	log, err := vlog.Open(datLog, true)
	if err != nil {
		seqFd.Close()
		return nil, err
	}

	idxPath := filepath.Join(dir, "idx.log")
	idx, err := index.Load(idxPath)
	if err != nil {
		return nil, errors.Join(
			err,
			seqFd.Close(),
			log.Close(),
		)
	}

	idxLog, err := index.NewWriter(idxPath, true)
	if err != nil {
		return nil, errors.Join(
			err,
			seqFd.Close(),
			log.Close(),
		)
	}

	return &UnackedStore{
		dir:    dir,
		log:    log,
		idx:    idx,
		idxLog: idxLog,
		seq:    uint64(seq),
		seqFd:  seqFd,
	}, nil
}

func (us *UnackedStore) Push(items timeq.Items) (uint64, error) {
	us.mu.Lock()
	defer us.mu.Unlock()

	loc, err := us.log.Push(items)
	if err != nil {
		return 0, err
	}

	loc.Key = timeq.Key(us.seq)
	us.seq++

	// Sync the current sequence number to disk to make sure
	// we start with the proper one. This could be probably done faster,
	// but it's probably okay for now.
	if err := us.seqFd.Truncate(0); err != nil {
		return 0, err
	}

	if _, err := us.seqFd.WriteString(strconv.FormatUint(us.seq, 10)); err != nil {
		return 0, err
	}

	if err := us.seqFd.Sync(); err != nil {
		return 0, err
	}

	if err := us.idxLog.Push(loc, us.idx.Trailer()); err != nil {
		return 0, err
	}

	us.idx.Set(loc)
	return uint64(loc.Key), nil
}

func (us *UnackedStore) Len() uint64 {
	us.mu.Lock()
	defer us.mu.Unlock()

	return uint64(us.idx.Len())
}

func (us *UnackedStore) Ack(id uint64) (int, error) {
	us.mu.Lock()
	defer us.mu.Unlock()

	// delete the referenced batch from the index
	// and push a marker to the idx log that indicate it was
	// deleted (used during reconstruction of the log.)

	// TODO: Delete() should return the deleted loc, so we can
	// figure out the number of items we acknowledged.
	us.idx.Delete(timeq.Key(id))
	return 0, us.idxLog.Push(item.Location{
		Key: timeq.Key(id),
		Off: 0,
		Len: 0, // len=0 means deleted batch.
	}, us.idx.Trailer())
}

// Clear will clear out all still unacknowledged messages from the store
// and call `fn` for each of them. It returns the number of unacked messages
// and, possibly, an error.
func (us *UnackedStore) Clear(fn timeq.ReadFn) (int, error) {
	us.mu.Lock()
	defer us.mu.Unlock()

	iter := us.idx.Iter()
	items := make(timeq.Items, 0, 2000)
	unacked := 0

	for iter.Next() {
		loc := iter.Value()
		if loc.Len == 0 {
			// acked batch.
			continue
		}

		items = items[:0]
		logIter := us.log.At(loc, true)
		for logIter.Next() {
			items = append(items, logIter.Item())
		}

		if err := logIter.Err(); err != nil {
			return unacked, err
		}

		unacked += len(items)

		if fn != nil {
			if err := fn(items); err != nil {
				return unacked, err
			}
		}
	}

	// clear the directory, since we finished clearing:
	return unacked, errors.Join(
		os.RemoveAll(us.dir),
		os.MkdirAll(us.dir, 0700),
	)
}

func (us *UnackedStore) Close() error {
	us.mu.Lock()
	defer us.mu.Unlock()

	return errors.Join(
		us.idxLog.Close(),
		us.log.Close(),
	)
}
