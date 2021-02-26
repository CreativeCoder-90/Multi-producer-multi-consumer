import com.model.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.SerializationException
import java.util.*

class CustomConsumers {
    companion object {
        var props: Properties = Properties()

        fun runConsumer1() {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER_GROUP)
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL)
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_STRING_DESERIALIZER)
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)
            try {
                var consumer1 = KafkaConsumer<String, User>(props)
                consumer1.subscribe(listOf(KafkaConstants.TOPIC1))
                var records: ConsumerRecords<String, User> = consumer1.poll(100)
                println("records consumed by consumer 1 from topic ${KafkaConstants.TOPIC1}")
                for (record in records) {
                    var key = record.key()
                    var value = record.value()
                    println("key: $key, value: $value")
                }
            } catch (e: SerializationException) {
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }

        fun runConsumer2() {
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER)
            props.put(ConsumerConfig.GROUP_ID_CONFIG, KafkaConstants.CONSUMER_GROUP)
            props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
            props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000")
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL)
            props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_STRING_DESERIALIZER)
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer::class.java)
            props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true)

            try {
                var consumer2 = KafkaConsumer<String, User>(props)
                consumer2.subscribe(listOf(KafkaConstants.TOPIC1))
                println("records consumed by consumer 2 from topic ${KafkaConstants.TOPIC1}")
                while (true) {
                    var records: ConsumerRecords<String, User> = consumer2.poll(100)
                    for (record in records) {
                        var key = record.key()
                         if (key.substring(0, 2).equals("P2")){
                        var value = record.value()
                        println("key: $key, value: $value")
                         }
                    }
                }
            } catch (e: SerializationException) {
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
    }
    }
