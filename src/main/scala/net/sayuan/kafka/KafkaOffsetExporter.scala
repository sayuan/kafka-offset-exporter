package net.sayuan.kafka

import java.util.concurrent.{Executors, TimeUnit}

import io.prometheus.client.exporter.MetricsServlet
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{ServletContextHandler, ServletHolder}

case class Config(zookeeper: String = "localhost:2181", interval: Int = 60, port: Int = 9991)

object KafkaOffsetExporter {
  def main(args: Array[String]) {
    val parser = new scopt.OptionParser[Config]("kafka-offset-exporter") {
      head("kafka-offset-exporter")
      opt[String]("zookeeper") valueName("<zk1>,<zk2>...") action { (x,c) => c.copy(zookeeper = x) } text("ZooKeeper connect string.")
      opt[Int]("interval") action { (x,c) => c.copy(interval = x) } text("update interval in seconds.")
      opt[Int]("port") action { (x,c) => c.copy(port = x) } text("port number.")
      help("help") text("prints this usage text")
    }
    parser.parse(args, Config()) match {
      case Some(config) => start(config)
      case _ =>
    }
  }

  def start(config: Config) {
    val server = new Server(config.port)
    val context = new ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addServlet(new ServletHolder(
      new MetricsServlet()), "/metrics")

    val scheduler = Executors.newScheduledThreadPool(1)
    scheduler.scheduleAtFixedRate(new KafkaOffsetUpdater(config), 0, config.interval, TimeUnit.SECONDS)

    server.start()
    server.join()
  }
}
