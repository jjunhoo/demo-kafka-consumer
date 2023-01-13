import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 컨슈머의 안전한 종료 (WakeupException)
 */
public class ConsumerWithSyncOffsetCommitShutdownHook {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "localhost:9092"; // 카프카 브로커
    private final static String GROUP_ID = "test-group"; // 그룹ID
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        // * Runtime 에 ShutdownHook 추가
        Runtime.getRuntime().addShutdownHook(new ShutdownThread());

        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Auto Commit 비활성화 (기본값 : true)

        consumer = new KafkaConsumer<>(configs);
        consumer.subscribe(Arrays.asList(TOPIC_NAME));

        // 무한 루프를 통한 토픽 Poll
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {
                    logger.info("record:{}", record);
                }

                consumer.commitSync();
            }
        } catch (WakeupException e) {
            logger.warn("Wakeup consumer");
        } finally {
            logger.info("Consumer close");
            consumer.close(); // 컨슈머 리소스 해제
        }
    }

    // 인텔리제이 터미널을 통해 Shutdown
    /*
     * 1. 해당 Java Application 프로세스 확인 (PID)
     *    ps -ef | grep ConsumerWithSyncOffsetCommitShutdownHook
     * 2. 해당 Java Application 프로세스 종료
     *    kill -term 50585
     * 3. 콘솔 확인
     *    [Thread-0] INFO SimpleConsumer - Shutdown Hook
     *    [main] WARN SimpleConsumer - Wakeup consumer
     *    [main] INFO SimpleConsumer - Consumer close
     */
    static class ShutdownThread extends Thread {
        public void run() {
            logger.info("Shutdown Hook");
            consumer.wakeup(); // Shutdown 시 wakeup 실행
        }
    }
}
