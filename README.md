# Kafka Offset Exporter

![screenshot](https://github.com/sayuan/kafka-offset-exporter/raw/master/screenshot.png)

## Build

```
git@github.com:sayuan/kafka-offset-exporter.git
cd kafka-offset-exporter
sbt assembly
```

## Run

Prepare a config file as follow
```
{
  "zookeeper": "zookeeper1:2181,zookeeper2:2181,zookeeper3:2181",
  "interval": 60,
  "port": 9991,
  "topicGroups": {
    "topic1": ["group1", "group2"],
    "topic2": ["group3"]
  }
}
```

Run it
```
java -jar target/scala-2.11/kafka-offset-exporter-assembly-0.0.1.jar
```

Check the metric at http://localhost:9991/metrics
