package com.bkatwal.kafka.util;

import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;

import com.bkatwal.kafka.impl.CustomAckMessageListener;
import com.bkatwal.kafka.impl.CustomMessageListener;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.AbstractMessageListenerContainer.AckMode;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.config.ContainerProperties;

/** @author "Bikas Katwal" 13/03/19 */
@Slf4j
public final class KafkaConsumerUtil {

  private static Map<String, ConcurrentMessageListenerContainer<String, String>> consumersMap =
      new HashMap<>();

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static void startOrCreateConsumers(
      final String topic,
      final Object messageListener,
      final int concurrency,
      final Map<String, Object> consumerProperties) {

    log.info("creating kafka consumer for topic {}", topic);

    ConcurrentMessageListenerContainer<String, String> container = consumersMap.get(topic);
    if (container != null) {
      if (!container.isRunning()) {
        log.info("Consumer already created for topic {}, starting consumer!!", topic);
        container.start();
        log.info("Consumer for topic {} started!!!!", topic);
      }
      return;
    }

    ContainerProperties containerProps = new ContainerProperties(topic);

    containerProps.setPollTimeout(100);
    Boolean enableAutoCommit = (Boolean) consumerProperties.get(ENABLE_AUTO_COMMIT_CONFIG);
    if (!enableAutoCommit) {
      containerProps.setAckMode(AckMode.MANUAL_IMMEDIATE);
    }

    ConsumerFactory<String, String> factory = new DefaultKafkaConsumerFactory<>(consumerProperties);

    container = new ConcurrentMessageListenerContainer<>(factory, containerProps);

    if (enableAutoCommit && !(messageListener instanceof CustomMessageListener)) {
      throw new IllegalArgumentException(
          "Expected message listener of type com.bkatwal.kafka.impl.CustomMessageListener!");
    }

    if (!enableAutoCommit && !(messageListener instanceof CustomAckMessageListener)) {
      throw new IllegalArgumentException(
          "Expected message listener of type com.bkatwal.kafka.impl.CustomAckMessageListener!");
    }

    container.setupMessageListener(messageListener);

    if (concurrency == 0) {
      container.setConcurrency(1);
    } else {
      container.setConcurrency(concurrency);
    }

    container.start();

    consumersMap.put(topic, container);

    log.info("created and started kafka consumer for topic {}", topic);
  }

  public static void stopConsumer(final String topic) {
    log.info("stopping consumer for topic {}", topic);
    ConcurrentMessageListenerContainer<String, String> container = consumersMap.get(topic);
    container.stop();
    log.info("consumer stopped!!");
  }

  private KafkaConsumerUtil() {
    throw new UnsupportedOperationException("Can not instantiate KafkaConsumerUtil");
  }
}
