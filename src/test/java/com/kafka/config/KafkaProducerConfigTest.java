package com.kafka.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class KafkaProducerConfigTest  {


    @Test
    public void testBuild_mapsCorrectly() {
        String kafkaUserName = "kafkaUserName";
        String kafkaPassword = "kafkaPassword";
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";
        String userInfo = "user:password";
        KafkaProducerConfig config = new KafkaProducerConfig(kafkaUserName, kafkaPassword, bootstrapServers, schemaRegistryUrl, userInfo);
        assertEquals(bootstrapServers, config.build().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        assertEquals("io.confluent.kafka.serializers.KafkaAvroSerializer", config.build().get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
        assertEquals("io.confluent.kafka.serializers.KafkaAvroSerializer", config.build().get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
        assertEquals("SASL_SSL", config.build().get("security.protocol"));
        assertEquals("PLAIN", config.build().get("sasl.mechanism"));
        assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + kafkaUserName + "\" password=\"" + kafkaPassword + "\";", config.build().get("sasl.jaas.config"));
        assertEquals(schemaRegistryUrl, config.build().get("schema.registry.url"));
        assertEquals("USER_INFO", config.build().get("schema.registry.basic.auth.credentials.source"));
        assertEquals(userInfo, config.build().get("basic.auth.user.info"));
    }

    @Test
    public void testBuild_validate_pass() {
        String kafkaUserName = "kafkaUserName";
        String kafkaPassword = "kafkaPassword";
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";
        String userInfo = "user:password";
        KafkaProducerConfig config = new KafkaProducerConfig(kafkaUserName, kafkaPassword, bootstrapServers, schemaRegistryUrl, userInfo);
        assertTrue(config.validateConfigs());
    }


    @Test
    public void testBuild_validate_fail() {
        assertFalse(new KafkaProducerConfig(null, "", "", "", "").validateConfigs());
        assertFalse(new KafkaProducerConfig("", null, "", "", "").validateConfigs());
        assertFalse(new KafkaProducerConfig("", "", null, "", "").validateConfigs());
        assertFalse(new KafkaProducerConfig("", "", "", null, "").validateConfigs());
        assertFalse(new KafkaProducerConfig("", "", "", "", null).validateConfigs());
    }


    @Test
    public void testBuild_failsWithMissingConfig() {
        String kafkaPassword = "kafkaPassword";
        String bootstrapServers = "localhost:9092";
        String schemaRegistryUrl = "http://localhost:8081";
        String userInfo = "user:password";
        KafkaProducerConfig config = new KafkaProducerConfig(null, kafkaPassword, bootstrapServers, schemaRegistryUrl, userInfo);
        assertThrows(RuntimeException.class, config::build);
    }
}