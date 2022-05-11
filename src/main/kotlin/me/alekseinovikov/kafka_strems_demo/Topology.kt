package me.alekseinovikov.kafka_strems_demo

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.serialization.Serializer
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KTable
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.state.Stores
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.CommandLineRunner
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.math.BigDecimal


@Component
class Topology(
    private val props: TopicProperties,
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) : CommandLineRunner {

    private val PRICE_SERDE = Serdes.serdeFrom(
        { _, data: Price -> objectMapper.writeValueAsBytes(data) },
        { _, data: ByteArray -> objectMapper.readValue(data, Price::class.java) }
    )

    private val STATE_SERDE = Serdes.serdeFrom(
        { _, data: CurrentPrices -> objectMapper.writeValueAsBytes(data) },
        { _, data: ByteArray -> objectMapper.readValue(data, CurrentPrices::class.java) }
    )

    private val KEY_SERDE = Serdes.String()

    @Autowired
    fun buildPipeline(streamsBuilder: StreamsBuilder) {
        val messageStream = streamsBuilder
            .stream(props.inputTopic, Consumed.with(KEY_SERDE, PRICE_SERDE))

        val aggregatedPrices: KTable<String, CurrentPrices> = messageStream
            .groupByKey()
            .aggregate({ CurrentPrices() }, { _, newPrice, state ->
                val currentPrice = state.prices[newPrice.selectionId] ?: BigDecimal.ZERO
                state.prices[newPrice.selectionId] = currentPrice.plus(newPrice.price ?: BigDecimal.ZERO)

                return@aggregate state
            }, Materialized.`as`<String, CurrentPrices>(Stores.persistentKeyValueStore("aggregated"))
                .withKeySerde(KEY_SERDE)
                .withValueSerde(STATE_SERDE)
            )

        aggregatedPrices
            .toStream()
            .mapValues { value -> objectMapper.writeValueAsString(value) }
            .to(props.outputTopic)
    }

    override fun run(vararg args: String?) {
        val price1 = Price(1L, 1L, 1.2.toBigDecimal())
        val price2 = Price(1L, 2L, 0.0.toBigDecimal())
        val price3 = Price(1L, 3L, null)

        val price4 = Price(1L, 1L, 12.0.toBigDecimal())
        val price5 = Price(2L, 2L, 1.1.toBigDecimal())
        val price6 = Price(1L, 3L, 3.2.toBigDecimal())

        val allPrices = listOf(price1, price2, price3, price4, price5, price6)

        allPrices.forEach { price ->
            val payload = objectMapper.writeValueAsString(price)
            val eventId = price.eventId.toString()

            kafkaTemplate.send(props.inputTopic, eventId, payload)
        }
    }

    @KafkaListener(topics = ["#{topicProperties.outputTopic}"], groupId = "my-app")
    fun listener(@Payload payload: String, @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: String) {
        val eventId = key.toLong()
        val currentPrices = objectMapper.readValue(payload, CurrentPrices::class.java)

        println("Event id: $eventId with data: $currentPrices")
    }

    data class Price(val eventId: Long, val selectionId: Long, val price: BigDecimal?)
    data class CurrentPrices(val prices: MutableMap<Long, BigDecimal?> = mutableMapOf())

}