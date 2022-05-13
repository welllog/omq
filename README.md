# omq
message

###### Currently only redis-based message queues are implemented

#### USAGE

```go
queue := redisq.NewQueue(redisClient, "queue_name", rediq.WithPartitionNum(4))
queue.Produce(&omq.Message{
    Topic: "topic_name",
    Payload: NewPayloadJsonEncoder(map[string]string{"name": "test"}),
    DelayAt: time.Now().Add(time.Second)
})
queue.Produce(&omq.Message{
    Topic: "topic_name",
    Payload: ByteEncoder("words"),
})
ctx, cancel := context.WithCancel(context.Background())
fetcher, _ := queue.Fetcher(ctx, 0)

quit := make(chan os.Signal)
signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
go func() {
    <-quit
    cancel()
}()

for msg := range fetcher.Messages() {
    // TODO
    fetcher.Commit(ctx, msg)
}
```

#### FEATURE
Program crashes usually result in lost messages, to avoid this, the library supports safe mode
```go
queue := redisq.NewQueue(redisClient, "queue_name", redisq.WithSafeMode(), redisq.WithCommitTimeout(10))
```