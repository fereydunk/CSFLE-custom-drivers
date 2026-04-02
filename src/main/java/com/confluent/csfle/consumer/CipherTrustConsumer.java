package com.confluent.csfle.consumer;

import com.confluent.csfle.Config;
import com.confluent.csfle.driver.CipherTrustKmsDriver;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class CipherTrustConsumer {

    public static void main(String[] args) {

        // 1. Register CipherTrust KMS driver
        CipherTrustKmsDriver.register(Config.CT_USERNAME, Config.CT_PASSWORD);

        // 2. Configure consumer
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.CONSUMER_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + Config.KAFKA_API_KEY + "\" " +
                "password=\"" + Config.KAFKA_API_SECRET + "\";");
        // Schema Registry
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SR_URL);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", Config.SR_API_KEY + ":" + Config.SR_API_SECRET);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        // CipherTrust config passed to KMS driver
        props.put("ciphertrust.username", Config.CT_USERNAME);
        props.put("ciphertrust.password", Config.CT_PASSWORD);

        System.out.println("Consuming from '" + Config.TOPIC + "' with CSFLE decryption via CipherTrust...\n");

        // 3. Consume and decrypt
        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Config.TOPIC));

            int emptyPolls = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                records.forEach(record -> {
                    GenericRecord user = record.value();
                    System.out.printf("Decrypted → name=%s email=%s ssn=%s credit_card=%s%n",
                            user.get("name"),
                            user.get("email"),
                            user.get("ssn"),
                            user.get("credit_card"));
                });
            }
        }

        System.out.println("\nDone.");
    }
}
