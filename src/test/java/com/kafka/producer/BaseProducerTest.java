package com.kafka.producer;

import com.kafka.config.KafkaProducerConfig;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class BaseProducerTest  {

    private KafkaProducer<SpecificRecordBase, SpecificRecordBase>   kafkaProducer;

    private BaseProducer baseProducer;

    @BeforeEach
    public void setUp() throws Exception {
        kafkaProducer = mock(KafkaProducer.class);
        baseProducer = new BaseProducer(kafkaProducer);
    }

    @Test
    public void testProduce_validatesConfigs() {
        assertThrows(RuntimeException.class, ()-> new BaseProducer(new KafkaProducerConfig(null, null, null, null, null)));
    }

    @Test
    public void testProduce_sends_flush_closes() {
        org.apache.avro.specific.SpecificRecordBase key = mock(org.apache.avro.specific.SpecificRecordBase.class);
        org.apache.avro.specific.SpecificRecordBase value = mock(org.apache.avro.specific.SpecificRecordBase.class);
        baseProducer.produce("topic", key, value, (errorMessage) -> {
            System.out.println("Error occurred: " + errorMessage);
            fail();
        });
        ProducerRecord<SpecificRecordBase, SpecificRecordBase> message = new ProducerRecord<>("topic", key, value);
        verify(kafkaProducer).send(eq(message));
        verify(kafkaProducer).flush();
        verify(kafkaProducer).close();
    }

    @Test
    public void testProduce_passesErrorInfoToCaller() {
        org.apache.avro.specific.SpecificRecordBase key = mock(org.apache.avro.specific.SpecificRecordBase.class);
        org.apache.avro.specific.SpecificRecordBase value = mock(org.apache.avro.specific.SpecificRecordBase.class);
        String error = "Error occurred";
        when(kafkaProducer.send(any(ProducerRecord.class))).thenThrow(new RuntimeException(error));
        baseProducer.produce("topic", key, value, (errorMessage) -> {
            assertEquals(error, errorMessage);
        });
    }
}