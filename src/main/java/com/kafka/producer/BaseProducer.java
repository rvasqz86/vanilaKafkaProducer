package com.kafka.producer;

import com.kafka.config.KafkaProducerConfig;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class BaseProducer {
    private final KafkaProducer<SpecificRecordBase, SpecificRecordBase> publisher;
    private final  ExecutorService executor = Executors.newSingleThreadExecutor();

    BaseProducer(KafkaProducer<SpecificRecordBase, SpecificRecordBase>  publisher) {
        this.publisher = publisher;
    }

    public BaseProducer(KafkaProducerConfig config) {
        if(!config.validateConfigs()){
            throw new RuntimeException("Invalid Kakfa configurations");
        }
        this.publisher = new KafkaProducer<>(config.build());
    }

    public void produce(String topic, org.apache.avro.specific.SpecificRecordBase key, org.apache.avro.specific.SpecificRecordBase value, Consumer<String> onError) {
        try {
            executor.execute(() -> {
                try {
                    ProducerRecord<SpecificRecordBase, SpecificRecordBase> message = new ProducerRecord<>(topic, key, value);
                    publisher.send(message);
                    publisher.flush();
                    publisher.close();
                } catch (Exception e) {
                    onError.accept(e.getMessage());
                }
            });
        } catch (Exception e) {
            onError.accept(e.getMessage());
        }
    }
}