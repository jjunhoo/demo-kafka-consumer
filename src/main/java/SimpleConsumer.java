import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SimpleConsumer {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "localhost:9092"; // 카프카 브로커
    private final static String GROUP_ID = "test-group"; // 그룹ID

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // subscribe
        consumer.subscribe(Arrays.asList(TOPIC_NAME)); // subscribe 로 특정 토픽을 구독하기 때문에 반드시 GROUP ID 필요

        // 무한 루프를 통한 토픽 Poll
        while (true) {
            // * poll 을 호출하는 시간이 길어지면, 리밸런싱 발생 가능 (옵션 설정 확인 필요)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
                // kafka-console.producer.sh - CLI 를 통한 메시지 발행
                // key : null, value : hello
                // [main] INFO SimpleConsumer - record:ConsumerRecord(topic = test, partition = 2, leaderEpoch = 0, offset = 0, CreateTime = 1673626096065, serialized key size = -1, serialized value size = 5, headers = RecordHeaders(headers = [], isReadOnly = false), key = null, value = hello)
            }
        }
    }
}
