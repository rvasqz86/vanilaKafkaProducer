package com.kafka;

class Consumer {
    private String topic;
    private String groupId;

    public void consume(long start, long end, String device){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
        properties.put("schema.registry.url", "http://localhost:8081");
        properties.put("basic.auth.credentials.source", "");
        properties.put("schema.registry.client.basic.auth.credentials.source", "USER_INFO");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        String topic = "";

        List<TopicPartition> partitions = new ArrayList<>();

        for(PartitionInfo partitionInfo : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(partitionInfo.topic(), partitionInfo.partition()));
        }

        consumer.assign(partitions);

        Map<TopicPartition,OffsetAndTimestamp> timestampsToSearch = new HashMap<>();

        for(TopicPartition partition : partitions) {
            OffsetAndTimestamp offsetAndTimestamp = consumer.offsetsForTimes.get(partition);
            consumer.seek(partition, offsetAndTimestamp.offset());
        }

        Set<TopicPartition> p = new HashSet<>();

        while(p){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
               if (record.timestamp() >= start && record.timestamp() <= end) {
                   System.out.printf("Consumed message from topic %s %s %s: key = %s, value = %s%n",
                           record.partition(), record.offset(), record.topic(), record.key(), record.value());
               }
            }
        }

        // Poll for new messages
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Consumed message from topic %s: key = %s, value = %s%n",
                        record.topic(), record.key(), record.value());
            }
        }
    }
}