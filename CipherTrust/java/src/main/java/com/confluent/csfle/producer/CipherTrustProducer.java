package com.confluent.csfle.producer;

import com.confluent.csfle.Config;
import com.confluent.csfle.driver.CipherTrustKmsDriver;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;

public class CipherTrustProducer {

    // Avro schema — confluent:tags mark fields for encryption
    private static final String SCHEMA_STR = """
        {
          "type": "record",
          "name": "User",
          "namespace": "com.confluent.csfle.demo",
          "fields": [
            {"name": "name",        "type": "string"},
            {"name": "email",       "type": "string"},
            {"name": "ssn",         "type": "string", "confluent:tags": ["PII"]},
            {"name": "credit_card", "type": "string", "confluent:tags": ["PII"]}
          ]
        }
        """;

    public static void main(String[] args) throws Exception {

        // 1. Register CipherTrust KMS driver
        CipherTrustKmsDriver.register(Config.CT_USERNAME, Config.CT_PASSWORD);

        // 2. Build Schema Registry client
        Map<String, String> srConf = new HashMap<>();
        srConf.put("basic.auth.credentials.source", "USER_INFO");
        srConf.put("basic.auth.user.info", Config.SR_API_KEY + ":" + Config.SR_API_SECRET);
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                List.of(Config.SR_URL), 100, srConf, null);

        // 3. Register schema with CSFLE encryption rule
        Rule encryptRule = new Rule(
                "encryptPII",
                null,
                RuleKind.TRANSFORM,
                RuleMode.WRITEREAD,
                FieldEncryptionExecutor.TYPE,
                Set.of("PII"),
                Map.of(
                        "encrypt.kek.name", Config.KEK_NAME,
                        "encrypt.kms.type", Config.KMS_TYPE,
                        "encrypt.kms.key.id", Config.KMS_KEY_ID
                ),
                null, null, "ERROR,NONE", false
        );
        RuleSet ruleSet = new RuleSet(null, List.of(encryptRule));

        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);
        AvroSchema schema = new AvroSchema(SCHEMA_STR, List.of(), Map.of(), null, ruleSet, null, true);

        String subject = Config.TOPIC + "-value";
        int schemaId = srClient.register(subject, schema);
        System.out.println("Schema registered: id=" + schemaId + ", subject=" + subject);

        // 4. Configure Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
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
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        // CipherTrust config passed to KMS driver
        props.put("ciphertrust.username", Config.CT_USERNAME);
        props.put("ciphertrust.password", Config.CT_PASSWORD);

        // 5. Produce records
        List<Map<String, String>> users = List.of(
                Map.of("name", "Alice Smith",    "email", "alice@example.com",  "ssn", "123-45-6789", "credit_card", "4111-1111-1111-1111"),
                Map.of("name", "Bob Johnson",    "email", "bob@example.com",    "ssn", "987-65-4321", "credit_card", "5500-0000-0000-0004"),
                Map.of("name", "Carol Williams", "email", "carol@example.com",  "ssn", "555-12-3456", "credit_card", "3400-0000-0000-009")
        );

        System.out.println("\nProducing " + users.size() + " messages to '" + Config.TOPIC + "' with CSFLE...\n");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            for (Map<String, String> user : users) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("name",        user.get("name"));
                record.put("email",       user.get("email"));
                record.put("ssn",         user.get("ssn"));
                record.put("credit_card", user.get("credit_card"));

                ProducerRecord<String, GenericRecord> kafkaRecord =
                        new ProducerRecord<>(Config.TOPIC, user.get("name"), record);

                producer.send(kafkaRecord, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("  Delivery failed: " + ex.getMessage());
                    } else {
                        System.out.printf("  Delivered → partition=%d offset=%d%n",
                                metadata.partition(), metadata.offset());
                    }
                });
                System.out.printf("  Produced (plaintext): name=%s ssn=%s cc=%s%n",
                        user.get("name"), user.get("ssn"), user.get("credit_card"));
            }
            producer.flush();
        }

        System.out.println("\nDone — ssn and credit_card encrypted via CipherTrust KEK.");
    }
}
