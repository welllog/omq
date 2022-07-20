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
Program crashes usually result in lost messages, to avoid this, the library supports retry, When this property is set, the task being processed will be cached.
Once the cached task exceeds the commitTimeout, it will re-enter the queue
```go
queue := redisq.NewQueue(redisClient, "queue_name", 
	redisq.WithMaxRetry(3), // Retry up to 3 times
	redisq.WithCommitTimeout(10), // When the number of retries is set greater than 0, re-enter the queue and retry if it is not submitted for more than 10 seconds
)
```