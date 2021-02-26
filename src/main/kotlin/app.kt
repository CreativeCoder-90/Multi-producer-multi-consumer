import java.util.*

fun main() {
    println("press required key")
    println("p1 -> producer1")
    println("p2 -> producer2")
    println("c1 -> consumer1")
    println("c2 -> consumer2")
    when(Scanner(System.`in`).next()){
        "p1"->CustomProducers.runProducer1()
        "p2"->CustomProducers.runProducer2()
        "c1"->CustomConsumers.runConsumer1()
        "c2"->CustomConsumers.runConsumer2()
    }
}