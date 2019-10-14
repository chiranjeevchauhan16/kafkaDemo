package com.github.chiri.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //create producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        //create producer record
        for(int i=0;i<10;i++) {

            String topic="firsttopic";
            String value="Welcome Chiri" + Integer.toString(i);
            String key="id_" + Integer.toString(i);

        ProducerRecord<String, String> record = new ProducerRecord<>(topic,key,value);
        logger.info("key" + key); // log the key
        //send data -asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                //executes everytime  a record is successfully sent or an exception is thrown
                if (e == null) {
                    // the record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp());

                } else {
                    logger.error("error while producing" + e);
                }
            }
        }).get(); //block the send to make it synchronous - don't do this in production
    }
        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
