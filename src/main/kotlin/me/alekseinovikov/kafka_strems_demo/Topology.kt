package me.alekseinovikov.kafka_strems_demo

import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Component


@Component
class Topology(private val props: TopicProperties) {

    private val STRING_SERDE = Serdes.String()

    @Autowired
    fun buildPipeline(streamsBuilder: StreamsBuilder) {
        val messageStream = streamsBuilder
            .stream(props.inputTopic, Consumed.with(STRING_SERDE, STRING_SERDE))

        val wordCount = messageStream.mapValues { _, v ->
            v.lowercase()
        }.flatMapValues { value ->
            value.split("\\W+")
        }.groupBy { key, _ ->
            key
        }.count()

        wordCount.toStream().to(props.outputTopic)
    }

}