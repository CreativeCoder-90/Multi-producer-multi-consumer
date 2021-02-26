import CustomConsumers.Companion.props
import com.model.User
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig
import io.confluent.kafka.serializers.KafkaAvroSerializer
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.SerializationException
import java.util.*

class CustomProducers {
    companion object {
        var props: Properties = Properties()
        fun runProducer1() {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER)
            props.put(ProducerConfig.ACKS_CONFIG, "all")
            props.put(ProducerConfig.RETRIES_CONFIG, 0)
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_STRING_SERIALIZER)
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL)
            try {
                var producer1 = KafkaProducer<String, User>(props)
                for (i in 0..100) {
                    var user: User =
                        User.newBuilder().setName("user$i").setAge(20 + i).setPanNumber("PAN$i$i$i$i$i").build()
                    var record = ProducerRecord<String, User>(KafkaConstants.TOPIC1, "P1$i", user)
                    producer1.send(record)
                   // Thread.sleep(500L)
                    println("produced$i")
                }
                producer1.flush()
                println("successfully produced records to the topic")
            } catch (e: SerializationException) {
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }

        fun runProducer2() {
            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.BOOTSTRAP_SERVER)
            props.put(ProducerConfig.ACKS_CONFIG, "all")
            props.put(ProducerConfig.RETRIES_CONFIG, 0)
            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaConstants.KAFKA_STRING_SERIALIZER)
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer::class.java)
            props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, KafkaConstants.SCHEMA_REGISTRY_URL)
            try {
                var producer2 = KafkaProducer<String, User>(props)
                for (i in 0..100) {
                    var user: User =
                        User.newBuilder().setName("user$i").setAge(20 + i).setPanNumber("PAN$i$i$i$i$i").build()
                    var record = ProducerRecord<String, User>(KafkaConstants.TOPIC1, "P2$i", user)
                    producer2.send(record)
                    //Thread.sleep(500L)
                    println("produced$i")
                }
                producer2.flush()
                println("successfully produced records to the topic")
            } catch (e: SerializationException) {
                e.printStackTrace()
            } catch (e: InterruptedException) {
                e.printStackTrace()
            }
        }
    }
}
