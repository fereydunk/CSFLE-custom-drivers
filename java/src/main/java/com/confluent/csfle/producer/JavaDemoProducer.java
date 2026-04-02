package com.confluent.csfle.producer;

import com.confluent.csfle.Config;
import com.confluent.csfle.driver.CipherTrustKmsDriver;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleKind;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Creates topic 'claude-test-ct-java', registers an Avro schema with a CSFLE
 * encryption rule, and produces 10 records with ssn + credit_card encrypted
 * via the CipherTrust KEK (poc-aes256-key).
 */
public class JavaDemoProducer {

    private static final String SCHEMA_STR = """
        {
          "type": "record",
          "name": "Employee",
          "namespace": "com.confluent.csfle.demo",
          "fields": [
            {"name": "id",          "type": "string"},
            {"name": "full_name",   "type": "string"},
            {"name": "email",       "type": "string"},
            {"name": "department",  "type": "string"},
            {"name": "ssn",         "type": "string", "confluent:tags": ["PII"]},
            {"name": "credit_card", "type": "string", "confluent:tags": ["PII"]},
            {"name": "salary",      "type": "string", "confluent:tags": ["PII"]}
          ]
        }
        """;

    private static final List<Map<String, String>> EMPLOYEES = List.of(
        Map.of("id", "E001", "full_name", "Alice Johnson",   "email", "alice.johnson@acme.com",   "department", "Engineering",  "ssn", "123-45-6789", "credit_card", "4111-1111-1111-1111", "salary", "145000"),
        Map.of("id", "E002", "full_name", "Bob Martinez",    "email", "bob.martinez@acme.com",    "department", "Finance",      "ssn", "234-56-7890", "credit_card", "5500-0000-0000-0004", "salary", "132000"),
        Map.of("id", "E003", "full_name", "Carol Williams",  "email", "carol.williams@acme.com",  "department", "HR",           "ssn", "345-67-8901", "credit_card", "3400-0000-0000-009",  "salary", "118000"),
        Map.of("id", "E004", "full_name", "David Chen",      "email", "david.chen@acme.com",      "department", "Engineering",  "ssn", "456-78-9012", "credit_card", "6011-0000-0000-0004", "salary", "158000"),
        Map.of("id", "E005", "full_name", "Emma Thompson",   "email", "emma.thompson@acme.com",   "department", "Marketing",    "ssn", "567-89-0123", "credit_card", "3530-1113-3330-0000", "salary", "125000"),
        Map.of("id", "E006", "full_name", "Frank Patel",     "email", "frank.patel@acme.com",     "department", "Legal",        "ssn", "678-90-1234", "credit_card", "4012-8888-8888-1881", "salary", "175000"),
        Map.of("id", "E007", "full_name", "Grace Kim",       "email", "grace.kim@acme.com",       "department", "Engineering",  "ssn", "789-01-2345", "credit_card", "5105-1051-0510-5100", "salary", "162000"),
        Map.of("id", "E008", "full_name", "Henry Okonkwo",   "email", "henry.okonkwo@acme.com",   "department", "Sales",        "ssn", "890-12-3456", "credit_card", "4222-2222-2222-2222", "salary", "110000"),
        Map.of("id", "E009", "full_name", "Isabella Rossi",  "email", "isabella.rossi@acme.com",  "department", "Product",      "ssn", "901-23-4567", "credit_card", "5431-1111-1111-1111", "salary", "148000"),
        Map.of("id", "E010", "full_name", "James Nakamura",  "email", "james.nakamura@acme.com",  "department", "Engineering",  "ssn", "012-34-5678", "credit_card", "6304-0000-0000-0000", "salary", "155000")
    );

    public static void main(String[] args) throws Exception {

        // 1. Ensure CipherTrust driver is on the classpath (loaded via ServiceLoader)
        CipherTrustKmsDriver.register(Config.CT_USERNAME, Config.CT_PASSWORD);

        // 2. Create topic
        createTopic();

        // 3. Build Schema Registry client
        Map<String, String> srConf = new HashMap<>();
        srConf.put("basic.auth.credentials.source", "USER_INFO");
        srConf.put("basic.auth.user.info", Config.SR_API_KEY + ":" + Config.SR_API_SECRET);
        SchemaRegistryClient srClient = new CachedSchemaRegistryClient(
                List.of(Config.SR_URL), 100, srConf, null);

        // 4. Register schema with a single CSFLE encryption rule covering all PII-tagged fields
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
        AvroSchema schema = new AvroSchema(SCHEMA_STR, List.of(), Map.of(), null, ruleSet, null, true);

        String subject = Config.JAVA_TOPIC + "-value";
        int schemaId = srClient.register(subject, schema);
        System.out.println("Schema registered: id=" + schemaId + ", subject=" + subject);

        // 5. Configure Kafka producer
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
        props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, Config.SR_URL);
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", Config.SR_API_KEY + ":" + Config.SR_API_SECRET);
        props.put(KafkaAvroSerializerConfig.AUTO_REGISTER_SCHEMAS, false);
        props.put(KafkaAvroSerializerConfig.USE_LATEST_VERSION, true);
        props.put("ciphertrust.username", Config.CT_USERNAME);
        props.put("ciphertrust.password", Config.CT_PASSWORD);

        // 6. Parse schema for record construction
        Schema avroSchema = new Schema.Parser().parse(SCHEMA_STR);

        System.out.println("\nProducing " + EMPLOYEES.size() + " messages to '" + Config.JAVA_TOPIC + "' with CSFLE...");
        System.out.println("  Encrypted fields: ssn, credit_card, salary (tagged PII)\n");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {
            for (Map<String, String> emp : EMPLOYEES) {
                GenericRecord record = new GenericData.Record(avroSchema);
                record.put("id",          emp.get("id"));
                record.put("full_name",   emp.get("full_name"));
                record.put("email",       emp.get("email"));
                record.put("department",  emp.get("department"));
                record.put("ssn",         emp.get("ssn"));
                record.put("credit_card", emp.get("credit_card"));
                record.put("salary",      emp.get("salary"));

                ProducerRecord<String, GenericRecord> kafkaRecord =
                        new ProducerRecord<>(Config.JAVA_TOPIC, emp.get("id"), record);

                producer.send(kafkaRecord, (metadata, ex) -> {
                    if (ex != null) {
                        System.err.println("  Delivery failed [" + emp.get("id") + "]: " + ex.getMessage());
                    } else {
                        System.out.printf("  [%s] Delivered → partition=%d offset=%d%n",
                                emp.get("id"), metadata.partition(), metadata.offset());
                    }
                });

                System.out.printf("  [%s] Plaintext  → name=%-20s ssn=%-13s cc=%-22s salary=%s%n",
                        emp.get("id"), emp.get("full_name"), emp.get("ssn"),
                        emp.get("credit_card"), emp.get("salary"));
            }
            producer.flush();
        }

        System.out.println("\nDone — ssn, credit_card, and salary encrypted via CipherTrust KEK '" + Config.KEK_NAME + "'.");
    }

    private static void createTopic() throws ExecutionException, InterruptedException {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, Config.BOOTSTRAP_SERVERS);
        adminProps.put("security.protocol", "SASL_SSL");
        adminProps.put("sasl.mechanism", "PLAIN");
        adminProps.put("sasl.jaas.config",
                "org.apache.kafka.common.security.plain.PlainLoginModule required " +
                "username=\"" + Config.KAFKA_API_KEY + "\" " +
                "password=\"" + Config.KAFKA_API_SECRET + "\";");

        try (AdminClient admin = AdminClient.create(adminProps)) {
            Set<String> existing = admin.listTopics().names().get();
            if (existing.contains(Config.JAVA_TOPIC)) {
                System.out.println("Topic '" + Config.JAVA_TOPIC + "' already exists — skipping creation.");
            } else {
                NewTopic newTopic = new NewTopic(Config.JAVA_TOPIC, 3, (short) 3);
                admin.createTopics(List.of(newTopic)).all().get();
                System.out.println("Topic '" + Config.JAVA_TOPIC + "' created (3 partitions, RF=3).");
            }
        }
    }
}
