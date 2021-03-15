package org.example.KafkaDemo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWirhCallbackandKey {

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWirhCallbackandKey.class);

        String bootStrapServers = "127.0.0.1:9092";
        // Create Producer Property

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Producer

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        // Create Producer Record

        ProducerRecord<String,String> record = new ProducerRecord<String,String>("first_topic","hello world");

        // Send Data Asynchronously
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // executes every time record is successfully sent or error generated
                if(e == null){
                    logger.info("Recieved new metadata \n" + "Topic:" + recordMetadata.topic() +"\n"
                    + "Partition:" + recordMetadata.partition() + "\n" +
                            "Offset:" + recordMetadata.offset() + "\n" +
                            "Timestamp: " + recordMetadata.timestamp());
                }
                else
                {
                    logger.error("Error while executing",e);
                }
            }
        });

        producer.flush();

        producer.close();
    }
}
