package net.sayuan.kafka

import java.io.File
import java.util

import io.prometheus.client.exporter.MetricsServlet
import javax.servlet.DispatcherType
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.{FilterHolder, ServletContextHandler}
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Arguments(config: String = "config.json")

object KafkaOffsetExporter {
  def main(args: Array[String]) {
    implicit val formats = DefaultFormats
    val parser = new scopt.OptionParser[Arguments]("kafka-offset-exporter") {
      head("kafka-offset-exporter")
      opt[String]("config") action { (x,c) => c.copy(config = x) } text("config file.")
      help("help") text("prints this usage text")
    }
    parser.parse(args, Arguments()) match {
      case Some(args) => {
        val config = parse(new File(args.config)).extract[Config]
        start(config)
      }
      case _ =>
    }
    start(null)
  }

  def start(config: Config) {
    val server = new Server(config.port)
    val context = new ServletContextHandler()
    context.setContextPath("/")
    server.setHandler(context)
    context.addFilter(new FilterHolder(new KafkaOffsetFilter(config)), "/metrics", util.EnumSet.of(DispatcherType.REQUEST))
    context.addServlet(classOf[MetricsServlet], "/metrics")

    server.start()
    server.join()
  }
}
