package org.example.KafkaDemo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

    public static void main(String[] args) {

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
        producer.send(record);

        producer.flush();

        producer.close();
    }
}
