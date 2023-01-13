import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ConsumerRebalanceListener 인터페이스 구현
 */
public class RebalanceListener implements ConsumerRebalanceListener {

    private final static Logger logger = LoggerFactory.getLogger(RebalanceListener.class);

    /**
     * 리밸런스가 시작되기 직전에 호출되는 메소드
     * @param partitions
     */
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are revoked : " + partitions.toString());
    }

    /**
     * 리밸런스가 끝난 뒤 파티션이 할당 완료되면 호출되는 메소드
     * @param partitions
     */
    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        logger.warn("Partitions are assigned : " + partitions.toString());
    }
}
