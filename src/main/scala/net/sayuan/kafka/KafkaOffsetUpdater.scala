package net.sayuan.kafka

import io.prometheus.client.Gauge
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.Logger

class KafkaOffsetUpdater (config: Config) extends Runnable {
  val logger = Logger.getLogger("net.sayuan.kafka.KafkaOffsetExporter")

  val logEndOffsetSizeGauge = Gauge.build().name("kafka_log_logendoffset").help("log end offset").labelNames("topic", "partition").register()
  val groupOffsetGauge = Gauge.build().name("kafka_group_offset").help("group offset").labelNames("topic", "partition", "group").register()
  val groupTimestampGauge = Gauge.build().name("kafka_group_timestamp").help("group timestamp").labelNames("topic", "partition", "group").register()

  def run() {
    val zkClient = new ZkClient(config.zookeeper, 30000, 30000, new ZKStringSerializer())
    val zkUtils = ZkUtils(zkClient, false)

    for (topic <- zkUtils.getAllTopics()) {
      for (pid <- zkUtils.getPartitionsForTopics(Seq(topic))(topic)) {
        val topicAndPartition = TopicAndPartition(topic, pid)

        zkUtils.getLeaderForPartition(topic, pid) match {
          case Some(bid) =>

            zkUtils.readDataMaybeNull(s"${ZkUtils.BrokerIdsPath}/$bid") match {
              case (Some(brokerInfoString), _) =>
                Json.parseFull(brokerInfoString) match {
                  case Some(m) =>
                    val brokerInfo = m.asInstanceOf[Map[String, Any]]
                    val host = brokerInfo.get("host").get.asInstanceOf[String]
                    val port = brokerInfo.get("port").get.asInstanceOf[Int]
                    val consumer = new SimpleConsumer(host, port, 10000, 100000, "KafkaOffsetExporter")
                    val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
                    val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
                    consumer.close()
                    logEndOffsetSizeGauge.labels(topic, pid.toString).set(logSize)
                  case None =>
                    logger.warn("Broker id %d does not exist".format(bid))
                }
              case (None, _) =>
                logger.warn("Broker id %d does not exist".format(bid))
            }
          case None =>
            logger.warn("No broker for partition %s - %s".format(topic, pid))
        }
      }
    }

    for (group <- zkUtils.getConsumerGroups();
         (topic, partitions) <- zkUtils.getPartitionsForTopics(zkUtils.getTopicsByConsumerGroup(group));
         pid <- partitions) {

      val (offset, stat) = zkUtils.readDataMaybeNull(s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
      if (offset.isDefined) {
        groupOffsetGauge.labels(topic, pid.toString, group).set(offset.get.toDouble)
        groupTimestampGauge.labels(topic, pid.toString, group).set(stat.getMtime)
      }
    }

    zkUtils.close()
  }
}
