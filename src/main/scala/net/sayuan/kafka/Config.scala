package net.sayuan.kafka

case class Config(
                   zookeeper: String = "localhost:2181",
                   port: Int = 9991,
                   topicGroups: Map[String, Set[String]]
                 )
