package com.github.chiri.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallBack {

    public static void main(String[] args) {

        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallBack.class);

        String bootstrapServers="127.0.0.1:9092";

        //create producer properties
        Properties properties =new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //create producer

        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create producer record

        ProducerRecord<String,String> record=new ProducerRecord<>("firsttopic","welcome chiri");

        //send data -asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                //executes everytime  a record is successfully sent or an exception is thrown
                if(e==null){
                    // the record was successfully sent
                    logger.info("Received new metadata. \n"+
                            "Topic: " + metadata.topic() + "\n"+
                            "Partition: " + metadata.partition() + "\n"+
                            "Offset: " + metadata.offset() +"\n"+
                            "Timestamp: " + metadata.timestamp());

                }
                else
                {
                    logger.error("error while producing"+e);
                }
            }
        });

        //flush data
        producer.flush();
        //flush and close
        producer.close();
    }
}
