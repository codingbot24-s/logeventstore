package broker2Helper

// When a follower restarts, it should:
// 1. Read its local log file to find the latest offset (last message index)
// 2. Call the leader’s /sync endpoint with that offset
// 3. The leader should respond with all messages after that offset
// 4. The follower then appends those to its local log.
// we can to this on another goroutine

//1.1 TODO: can we find the file pointer when we start the follower beacause topicMap is in memory so how can we read the log files??


func StartReadingLogFiles() {}