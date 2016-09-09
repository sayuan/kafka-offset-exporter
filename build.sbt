name := "kafka-offset-exporter"

version := "0.0.1"

organization := "net.sayuan"

scalaVersion := "2.11.7"

libraryDependencies += "org.apache.kafka" %% "kafka" % "0.9.0.1"
libraryDependencies += "org.eclipse.jetty" % "jetty-servlet" % "9.3.0.M2"
libraryDependencies += "io.prometheus" % "simpleclient" % "0.0.14"
libraryDependencies += "io.prometheus" % "simpleclient_hotspot" % "0.0.14"
libraryDependencies += "io.prometheus" % "simpleclient_servlet" % "0.0.14"
libraryDependencies += "joda-time" % "joda-time" % "2.3"
libraryDependencies += "com.github.scopt" %% "scopt" % "3.4.0"
libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.4.0"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = true)
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
