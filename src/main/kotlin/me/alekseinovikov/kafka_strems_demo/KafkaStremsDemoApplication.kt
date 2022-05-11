package me.alekseinovikov.kafka_strems_demo

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaStremsDemoApplication

fun main(args: Array<String>) {
    runApplication<KafkaStremsDemoApplication>(*args)
}
