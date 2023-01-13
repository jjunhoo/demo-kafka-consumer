import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 수동 비동기 오프셋 커밋 컨슈머
 */
public class ConsumerWithAsyncCommit {
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
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Auto Commit 비활성화 (기본값 : true)

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs);

        // subscribe
        consumer.subscribe(Arrays.asList(TOPIC_NAME)); // subscribe 로 특정 토픽을 구독하기 때문에 반드시 GROUP ID 필요

        // 무한 루프를 통한 토픽 Poll
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("record:{}", record);
            }

            // * ENABLE_AUTO_COMMIT_CONFIG : false 로 두었기 때문에 수동으로 커밋 처리
            // * 반드시 레코드 처리 이후 실행 필요 (리밸런싱 발생 시 처리 되지 않은 레코드부터 안전하게 데이터 처리 가능)
            consumer.commitAsync(new OffsetCommitCallback() {
                // commit 에 실패한 경우, 후속 처리
                // * 레코드 처리에 성공하고, 브로커와의 통신에 commit 이 실패한 경우, 이미 처리한 데이터를 다시 poll 할 수 있기 때문에 데이터 중복 컨슘 가능
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.out.println("Commit failed");
                    } else {
                        System.out.println("Commit succeeded");
                    }

                    if (exception != null) {
                        logger.error("Commit failed for offsets {}", offsets, exception);
                    }
                }
            });
        }
    }
}
