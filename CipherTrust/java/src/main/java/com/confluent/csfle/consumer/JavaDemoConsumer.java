package com.confluent.csfle.consumer;

import com.confluent.csfle.Config;
import com.confluent.csfle.driver.CipherTrustKmsDriver;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Consumes and decrypts records from 'claude-test-ct-java'.
 * CSFLE transparently decrypts ssn, credit_card, and salary fields
 * using the CipherTrust KEK (poc-aes256-key).
 */
public class JavaDemoConsumer {

    public static void main(String[] args) {

        CipherTrustKmsDriver.register(Config.CT_USERNAME, Config.CT_PASSWORD);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, Config.JAVA_CONSUMER_GROUP);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + Config.KAFKA_API_KEY + "\" " +
                "password=\"" + Config.KAFKA_API_SECRET + "\";");
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SR_URL);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", Config.SR_API_KEY + ":" + Config.SR_API_SECRET);
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        props.put("ciphertrust.username", Config.CT_USERNAME);
        props.put("ciphertrust.password", Config.CT_PASSWORD);

        System.out.println("Consuming from '" + Config.JAVA_TOPIC + "' with CSFLE decryption via CipherTrust...\n");

        try (KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(Config.JAVA_TOPIC));

            int emptyPolls = 0;
            int count = 0;
            while (emptyPolls < 3) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(5));
                if (records.isEmpty()) {
                    emptyPolls++;
                    continue;
                }
                emptyPolls = 0;
                for (var record : records) {
                    GenericRecord emp = record.value();
                    count++;
                    System.out.printf("[%s] Decrypted → name=%-20s dept=%-12s ssn=%-13s cc=%-22s salary=%s%n",
                            emp.get("id"),
                            emp.get("full_name"),
                            emp.get("department"),
                            emp.get("ssn"),
                            emp.get("credit_card"),
                            emp.get("salary"));
                }
            }
            System.out.println("\nTotal records decrypted: " + count);
        }

        System.out.println("Done.");
    }
}
