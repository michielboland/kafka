# Kafka Streams Quickstart

This project creates some small Kafka streams processors. Most of the code take from or inspired by the Apache Kafka website.

Before running, start a kafka broker on localhost:9092

- `CreateTopics` - creates the topics needed by these snippets
- `Writer` - sends initial set of messages, using processor API
- `WordCount` - count words in input messages, using streams DSL
- `Pipe` - copy events from one topic to another
- `LineSplit` - split messages into multiple other messages
- `States` - rather contrived example that demonstrates the processor API in combination with stateful processing

```
                    +--------------------------+
                    | streams-wordcount-output |
                    +--------------------------+
                                  ^
                                  |
                             WordCount
                                  ^
                                  |
+-------+              +-------------------------+            +---------------------+
| dummy | -> Writer -> | streams-plaintext-input | -> Pipe -> | streams-pipe-output |
+-------+              +-------------------------+            +---------------------+
                           |                  |
                           v                  v
                         States           LineSplit
                                              |
                                              v
                                    +--------------------------+          
                                    | streams-linesplit-output |        
                                    +--------------------------+
```
