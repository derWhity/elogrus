package elogrus

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gopkg.in/olivere/elastic.v5"
)

var (
	// ErrCannotCreateIndex is fired if index creation fails
	ErrCannotCreateIndex = fmt.Errorf("Cannot create index")
)

// IndexNameFunc defines a function that will dynamically create an index name
type IndexNameFunc func() string

type fireFunc func(entry *logrus.Entry, hook *ElasticHook, indexName string) error

type MessageCreatorFunc func(entry *logrus.Entry, hook *ElasticHook) interface{}

// ElasticHook is a logrus
// hook for ElasticSearch
type ElasticHook struct {
	client         *elastic.Client
	host           string
	index          IndexNameFunc
	levels         []logrus.Level
	ctx            context.Context
	ctxCancel      context.CancelFunc
	fireFunc       fireFunc
	messageCreator MessageCreatorFunc // Function to use when creating the message for Elasticsearch
}

// NewElasticHook creates new hook
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewElasticHook(client *elastic.Client, host string, level logrus.Level, index string) (*ElasticHook, error) {
	return NewElasticHookWithFunc(client, host, level, func() string { return index })
}

// NewAsyncElasticHook creates new  hook with asynchronous log
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// index - name of the index in ElasticSearch
func NewAsyncElasticHook(client *elastic.Client, host string, level logrus.Level, index string) (*ElasticHook, error) {
	return NewAsyncElasticHookWithFunc(client, host, level, func() string { return index })
}

// NewElasticHookWithFunc creates new hook with
// function that provides the index name. This is useful if the index name is
// somehow dynamic especially based on time.
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// indexFunc - function providing the name of index
func NewElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc) (*ElasticHook, error) {
	return newHookFuncAndFireFunc(client, host, level, indexFunc, syncFireFunc)
}

// NewAsyncElasticHookWithFunc creates new asynchronous hook with
// function that provides the index name. This is useful if the index name is
// somehow dynamic especially based on time.
// client - ElasticSearch client using gopkg.in/olivere/elastic.v5
// host - host of system
// level - log level
// indexFunc - function providing the name of index
func NewAsyncElasticHookWithFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc) (*ElasticHook, error) {
	return newHookFuncAndFireFunc(client, host, level, indexFunc, asyncFireFunc)
}

func newHookFuncAndFireFunc(client *elastic.Client, host string, level logrus.Level, indexFunc IndexNameFunc, fireFunc fireFunc) (*ElasticHook, error) {
	levels := []logrus.Level{}
	for _, l := range []logrus.Level{
		logrus.PanicLevel,
		logrus.FatalLevel,
		logrus.ErrorLevel,
		logrus.WarnLevel,
		logrus.InfoLevel,
		logrus.DebugLevel,
	} {
		if l <= level {
			levels = append(levels, l)
		}
	}

	ctx, cancel := context.WithCancel(context.TODO())

	// Use the IndexExists service to check if a specified index exists.
	exists, err := client.IndexExists(indexFunc()).Do(ctx)
	if err != nil {
		// Handle error
		cancel()
		return nil, err
	}
	if !exists {
		createIndex, err := client.CreateIndex(indexFunc()).Do(ctx)
		if err != nil {
			cancel()
			return nil, err
		}
		if !createIndex.Acknowledged {
			cancel()
			return nil, ErrCannotCreateIndex
		}
	}

	return &ElasticHook{
		client:         client,
		host:           host,
		index:          indexFunc,
		levels:         levels,
		ctx:            ctx,
		ctxCancel:      cancel,
		fireFunc:       fireFunc,
		messageCreator: defaultMessageCreator,
	}, nil
}

// Fire is required to implement
// Logrus hook
func (hook *ElasticHook) Fire(entry *logrus.Entry) error {
	return hook.fireFunc(entry, hook, hook.index())
}

func asyncFireFunc(entry *logrus.Entry, hook *ElasticHook, indexName string) error {
	go syncFireFunc(entry, hook, hook.index())
	return nil
}

// defaultMessageCreator is the default function used for Elasticsearch message creation
func defaultMessageCreator(entry *logrus.Entry, hook *ElasticHook) interface{} {
	level := entry.Level.String()
	return &struct {
		Host      string
		Timestamp string `json:"@timestamp"`
		Message   string
		Data      logrus.Fields
		Level     string
	}{
		hook.host,
		entry.Time.UTC().Format(time.RFC3339Nano),
		entry.Message,
		entry.Data,
		strings.ToUpper(level),
	}
}

func syncFireFunc(entry *logrus.Entry, hook *ElasticHook, indexName string) error {

	if e, ok := entry.Data[logrus.ErrorKey]; ok && e != nil {
		if err, ok := e.(error); ok {
			entry.Data[logrus.ErrorKey] = err.Error()
		}
	}

	msg := hook.messageCreator(entry, hook)

	_, err := hook.client.
		Index().
		Index(hook.index()).
		Type("log").
		BodyJson(msg).
		Do(hook.ctx)

	return err
}

// Levels is an interface function required for logrus
// hook implementation
func (hook *ElasticHook) Levels() []logrus.Level {
	return hook.levels
}

// Cancel will cancel all calls to elastic
func (hook *ElasticHook) Cancel() {
	hook.ctxCancel()
}

// SetMessageCreator changes the message creation function to the provided one
func (hook *ElasticHook) SetMessageCreator(fn MessageCreatorFunc) {
	hook.messageCreator = fn
}

// GetHost returns the host configured in the hook instance
func (hook *ElasticHook) GetHost() string {
	return hook.host
}
