package com.bkatwal.kafka.impl;

import com.bkatwal.kafka.api.ICustomJsonConverter;
import com.bkatwal.kafka.api.Subject;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/** @author "Bikas Katwal" 13/03/19 */
@AllArgsConstructor
public class CustomAckMessageListener implements AcknowledgingMessageListener<String, String> {

  private ICustomJsonConverter iCustomJsonConverter;

  private Subject kafkaMessageSubject;

  @Override
  public void onMessage(
      ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {

    final String finalMessage =
        iCustomJsonConverter != null
            ? iCustomJsonConverter.convert(consumerRecord.value())
            : consumerRecord.value();

    kafkaMessageSubject.notifyObservers(consumerRecord.topic(), finalMessage);

    acknowledgment.acknowledge();
  }
}
