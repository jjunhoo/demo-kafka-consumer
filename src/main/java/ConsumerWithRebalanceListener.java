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

public class ConsumerWithRebalanceListener {
    private final static Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);
    private final static String TOPIC_NAME = "test"; // 토픽명
    private final static String BOOTSTRAP_SERVERS = "localhost:9092"; // 카프카 브로커
    private final static String GROUP_ID = "test-group"; // 그룹ID

    // KafkaConsumer 전역 변수 설정
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false); // Auto Commit 비활성화 (기본값 : true)

        consumer = new KafkaConsumer<>(configs);
        // test 토픽에 대해 리밸런스 리스너가 추가된 것을 아래 콘솔 출력과 같이 확인 가능
        // 콘솔 출력 : [main] WARN RebalanceListener - Partitions are assigned : [test-1, test-0, test-3, test-2]
        /*
         * 컨슈머 어플리케이션을 2개 실행
         * 1. 인텔리제이 상단 'ConsumerWithRebalanceListener' 클릭 및 'Edit Configurations' 클릭
         * 2. build and rus 우측 'Modify Options' 클릭
         * 3. operating system 영역의 'Allow multiple instances' 클릭
         * 4. 해당 main 클래스 한번 더 실행
         * 5. 1번째 컨슈머 파티션 재할당 확인 (onPartitionsRevoked, onPartitionsAssigned)
         *    - 파티션이 4개인 경우 (1번째 컨슈머)
         *      - [main] WARN RebalanceListener - Partitions are assigned : [test-1, test-0, test-3, test-2]
         *         - 1번째 컨슈머만 실행 시 로그
         *      - [main] WARN RebalanceListener - Partitions are revoked : [test-1, test-0, test-3, test-2]
         *         - 파티션 재할당 전 호출
         *      - [main] WARN RebalanceListener - Partitions are assigned : [test-1, test-0]
         *         - 파티션 재할당 후 호출
         *         - 2번째 컨슈머 추가 실행 시 로그
         *    - 파티션이 4개인 경우 (2번째 컨슈머)
         *      - [main] WARN RebalanceListener - Partitions are assigned : [test-3, test-2]
         *         - 파티션 재할당 후 호출
         *         - 2번째 컨슈머 추가 실행 시 로그
         * 6. 1번째 컨슈머 종료 시 2번째 컨슈머의 콘솔 로그에서 파티션 재할당 확인 (onPartitionsAssigned)
         *    - 파티션이 4개인 경우 (1번째 컨슈머)
         *      - 컨슈머 종료
         *    - 파티션이 4개인 경우 (1번째 컨슈머)
         *      - [main] WARN RebalanceListener - Partitions are assigned : [test-1, test-0, test-3, test-2]
         *        - 파티션 재할당 후 호출
         *        - 2번째 컨슈머로 모든 파티션 재할당 후 로그
         */
        consumer.subscribe(Arrays.asList(TOPIC_NAME), new RebalanceListener()); // subscribe 시 파라미터로 리밸런스 리스너 추가

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("{}", record);
            }
        }
    }
}
