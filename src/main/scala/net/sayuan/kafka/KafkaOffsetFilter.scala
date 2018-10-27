package net.sayuan.kafka

import java.util.concurrent.Executors

import io.prometheus.client.Gauge
import javax.servlet._
import kafka.api.{OffsetFetchRequest, OffsetFetchResponse, OffsetRequest, PartitionOffsetRequestInfo}
import kafka.client.ClientUtils
import kafka.common.{OffsetMetadataAndError, TopicAndPartition}
import kafka.consumer.SimpleConsumer
import kafka.utils.{Json, ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import org.apache.kafka.common.protocol.Errors
import org.apache.log4j.{Level, Logger}

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

class KafkaOffsetFilter(config: Config) extends Filter {
  override def init(filterConfig: FilterConfig): Unit = ()
  override def destroy(): Unit = ()

  val logger = Logger.getLogger("net.sayuan.kafka.KafkaOffsetFilter")
  logger.setLevel(Level.INFO)

  val logEndOffsetSizeGauge = Gauge.build().name("kafka_log_logendoffset").help("log end offset").labelNames("topic", "partition").register()
  val groupOffsetGauge = Gauge.build().name("kafka_group_offset").help("group offset").labelNames("topic", "partition", "group").register()

  implicit val ec = new ExecutionContext {
    val threadPool = Executors.newFixedThreadPool(30)
    def execute(runnable: Runnable) = threadPool.submit(runnable)
    def reportFailure(t: Throwable) = t.printStackTrace()
  }

  val zkClient = new ZkClient(config.zookeeper, 30000, 30000, new ZKStringSerializer())
  val zkUtils = ZkUtils(zkClient, false)

  override def doFilter(request: ServletRequest, response: ServletResponse, chain: FilterChain): Unit = {
    var futures = List[Future[Unit]]()

    val groupTopics = mutable.Map[String, List[String]]().withDefaultValue(Nil)
    for (topic <- config.topicGroups.keys) {
      for (group <- config.topicGroups(topic)) {
        groupTopics(group) = topic :: groupTopics(group)
      }
    }

    for (group <- groupTopics.keys) {
      val topics = groupTopics(group)
      val topicPartitions = zkUtils.getPartitionsForTopics(topics).flatMap { case (topic, partitions) => partitions.map(TopicAndPartition(topic, _))}.toList

      val channel = ClientUtils.channelToOffsetManager(group, zkUtils, 600, 300)
      channel.send(OffsetFetchRequest(group, topicPartitions))
      val offsetFetchResponse = OffsetFetchResponse.readFrom(channel.receive().payload())
      channel.disconnect()

      offsetFetchResponse.requestInfo.foreach { case (topicAndPartition, offsetAndMetadata) =>
        futures = Future {
          val topic = topicAndPartition.topic
          val partition = topicAndPartition.partition
          offsetAndMetadata match {
            case OffsetMetadataAndError.NoOffset =>
              val topicDirs = new ZKGroupTopicDirs(group, topicAndPartition.topic)
              // this group may not have migrated off zookeeper for offsets storage (we don't expose the dual-commit option in this tool
              // (meaning the lag may be off until all the consumers in the group have the same setting for offsets storage)
              try {
                val offset = zkUtils.readData(topicDirs.consumerOffsetDir + "/" + topicAndPartition.partition)._1.toLong
                groupOffsetGauge.labels(topic, partition.toString, group).set(offset)
              } catch {
                case z: ZkNoNodeException =>
                  println(s"Could not fetch offset from zookeeper for group '$group' partition '$topicAndPartition' due to missing offset data in zookeeper.", z)
              }
            case offsetAndMetaData if offsetAndMetaData.error == Errors.NONE.code =>
              groupOffsetGauge.labels(topic, partition.toString, group).set(offsetAndMetaData.offset)
            case _ =>
              println(s"Could not fetch offset from kafka for group '$group' partition '$topicAndPartition' due to ${Errors.forCode(offsetAndMetadata.error).exception}.")
          }
        } :: futures
      }
    }

    for ((topic, partitions) <- zkUtils.getPartitionsForTopics(config.topicGroups.keys.toSeq)) {
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
      }
    }

    for (f <- futures) Await.result(f, Duration.Inf)
    chain.doFilter(request, response)
  }

}
