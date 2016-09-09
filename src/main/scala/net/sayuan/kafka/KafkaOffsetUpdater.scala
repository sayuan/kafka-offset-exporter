package net.sayuan.kafka

import java.util.concurrent.Executors

import io.prometheus.client.Gauge
import kafka.api.{OffsetRequest, PartitionOffsetRequestInfo}
import kafka.common.TopicAndPartition
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.log4j.{Level, Logger}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class KafkaOffsetUpdater(config: Config) extends Runnable {
  val logger = Logger.getLogger("net.sayuan.kafka.KafkaOffsetExporter")
  logger.setLevel(Level.INFO)

  val logEndOffsetSizeGauge = Gauge.build().name("kafka_log_logendoffset").help("log end offset").labelNames("topic", "partition").register()
  val groupOffsetGauge = Gauge.build().name("kafka_group_offset").help("group offset").labelNames("topic", "partition", "group").register()
  val groupTimestampGauge = Gauge.build().name("kafka_group_timestamp").help("group timestamp").labelNames("topic", "partition", "group").register()

  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(30)
    def execute(runnable: Runnable) = threadPool.submit(runnable)
    def reportFailure(t: Throwable) = t.printStackTrace()
  }

  val zkClient = new ZkClient(config.zookeeper, 30000, 30000, new ZKStringSerializer())
  val zkUtils = ZkUtils(zkClient, false)

  var counter = 0

  def run() {
    counter += 1
    logger.info(s"Start the $counter round")
    try {
      val topicGroups = config.topicGroups
      var futures = List[Future[Unit]]()

      for ((topic, partitions) <- zkUtils.getPartitionsForTopics(topicGroups.keys.toSeq)) {
        val groups = topicGroups(topic)

        for (pid <- partitions) {
          val topicAndPartition = TopicAndPartition(topic, pid)
          futures = Future {
            zkUtils.getLeaderForPartition(topic, pid) match {
              case Some(bid) =>
                zkUtils.readDataMaybeNull(s"${ZkUtils.BrokerIdsPath}/$bid") match {
                  case (Some(brokerInfoString), _) =>
                    Json.parseFull(brokerInfoString) match {
                      case Some(m) =>
                        val brokerInfo = m.asInstanceOf[Map[String, Any]]
                        val host = brokerInfo.get("host").get.asInstanceOf[String]
                        val port = brokerInfo.get("port").get.asInstanceOf[Int]
                        val consumer = new SimpleConsumer(host, port, 3000, 100000, "KafkaOffsetExporter")
                        val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(OffsetRequest.LatestTime, 1)))
                        val logSize = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPartition).offsets.head
                        consumer.close()
                        logEndOffsetSizeGauge.labels(topic, pid.toString).set(logSize)
                      case None => {
                        logger.error("Broker id %d does not exist".format(bid))
                      }
                    }
                  case (None, _) => {
                    logger.error("Broker id %d does not exist".format(bid))
                  }
                }
              case None => {
                logger.error("No broker for partition %s - %s".format(topic, pid))
              }
            }
          } :: futures

          for (group <- groups) {
            futures = Future {
              val (offset, stat) = zkUtils.readDataMaybeNull(s"${ZkUtils.ConsumersPath}/$group/offsets/$topic/$pid")
              if (offset.isDefined) {
                groupOffsetGauge.labels(topic, pid.toString, group).set(offset.get.toDouble)
                groupTimestampGauge.labels(topic, pid.toString, group).set(stat.getMtime)
              } else {
                logger.error("Group %s not exist for partition %s - %s".format(group, topic, pid))
              }
            } :: futures
          }
        }
      }

      for (f <- futures) Await.result(f, Duration.Inf)
      logger.info(s"Finish the $counter round")
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }
}
