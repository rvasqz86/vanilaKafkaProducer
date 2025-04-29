package com.kafka.config;


import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Map;

public class KafkaProducerConfig {
    private final String kafkaUserName;
    private final String kafkaPassword;
    private final String bootstrapServers;
    private final String schemaRegistryUrl;
    private final String getSchemaRegistryUserInfo;

    public KafkaProducerConfig(String kafkaUserName, String kafkaPassword, String bootstrapServers, String schemaRegistryUrl, String userInfo) {
        this.kafkaUserName = kafkaUserName;
        this.kafkaPassword = kafkaPassword;
        this.bootstrapServers = bootstrapServers;
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.getSchemaRegistryUserInfo = userInfo;
    }

    public boolean validateConfigs(){
        return kafkaUserName != null &&
                kafkaPassword != null &&
                bootstrapServers != null &&
                schemaRegistryUrl != null &&
                getSchemaRegistryUserInfo != null;
    }

    public Map<String, Object> build() {
        if (!validateConfigs()) {
            throw new RuntimeException("Invalid Kakfa configurations");
        }
        Map<String, Object> properties = new java.util.HashMap<>();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        String keySerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        String valueSerializer = "io.confluent.kafka.serializers.KafkaAvroSerializer";
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        properties.put("security.protocol", "SASL_SSL");
        properties.put("sasl.mechanism", "PLAIN");
        properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"" + kafkaUserName + "\" password=\"" + kafkaPassword + "\";");
        properties.put("schema.registry.url", schemaRegistryUrl);
        properties.put("schema.registry.basic.auth.credentials.source", "USER_INFO");
        properties.put("basic.auth.user.info", getSchemaRegistryUserInfo);
        return properties;
    }


}
