package me.alekseinovikov.kafka_strems_demo

import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsConfig.*
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.kafka.annotation.EnableKafkaStreams
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.TopicBuilder


@EnableKafka
@EnableKafkaStreams
@Configuration
class KafkaStreamsConfig {

    @Value(value = "\${spring.kafka.bootstrap-servers}")
    private val bootstrapAddress: String? = null

    @Bean
    fun defaultKafkaStreamsConfig(): KafkaStreamsConfiguration {
        val props: MutableMap<String, Any> = HashMap()
        props[APPLICATION_ID_CONFIG] = "streams-demo-app"
        props[BOOTSTRAP_SERVERS_CONFIG] = bootstrapAddress!!
        props[DEFAULT_KEY_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name
        props[DEFAULT_VALUE_SERDE_CLASS_CONFIG] = Serdes.String().javaClass.name

        return KafkaStreamsConfiguration(props)
    }

    @Bean
    fun inputTopic(props: TopicProperties) = TopicBuilder
        .name(props.inputTopic)
        .build()

    @Bean
    fun outputTopic(props: TopicProperties) = TopicBuilder
        .name(props.outputTopic)
        .build()

}