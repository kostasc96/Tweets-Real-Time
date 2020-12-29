package gr.kostas.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
    final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

    String bootstrapServers = "127.0.0.1:9092";

    //create Producer properties
    Properties properties = new Properties();
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer .class.getName());
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    //create the producer
    final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        for(int i=0;i<10;i++) {
        //create a producer record
        String topic = "first_topic";
        String value = "Hello World " + Integer.toString(i);
        String key = "id_" + Integer.toString(i);

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(topic, key, value);

        logger.info(key);  //log the key
        //id_0 goes to partition 1
        //id_1 goes to partition 0
        //id_2 goes to partition 2
        //id_3 goes to partition 0
        //id_4 goes to partition 2
        //id_5 goes to partition 2
        //id_6 goes to partition 0
        //id_7 goes to partition 2
        //id_8 goes to partition 1
        //id_9 goes to partition 2

        //send data - asynchronous
        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                //executes every time a record is successfully sent or an exeption is thrown
                if (e == null) {
                    //if record is successfully sent
                    logger.info("Received new Metadata.\n" +
                            "Topic: " + recordMetadata.topic() + "\n" +
                            "Partition: " + recordMetadata.partition() + "\n" +
                            "Offset: " + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing", e);
                }
            }
        }).get();   // make .send() synchronous but bad practice
    }

    //flush data
        producer.flush();

    //flush and close producer
        producer.close();
}
}
