package me.alekseinovikov.kafka_strems_demo

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.stereotype.Component

@Component
@ConfigurationProperties(prefix = "kafka.topics")
class TopicProperties {

    var inputTopic: String = "input-topic"
    var outputTopic: String = "output-topic"

}