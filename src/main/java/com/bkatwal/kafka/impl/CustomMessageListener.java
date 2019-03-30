package com.bkatwal.kafka.impl;

import com.bkatwal.kafka.api.ICustomJsonConverter;
import com.bkatwal.kafka.api.Subject;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.MessageListener;

/** @author "Bikas Katwal" 13/03/19 */
@AllArgsConstructor
public class CustomMessageListener implements MessageListener<String, String> {

  private ICustomJsonConverter ICustomJsonConverter;

  private Subject kafkaMessageSubject;

  @Override
  public void onMessage(ConsumerRecord<String, String> consumerRecord) {

    final String finalMessage =
        ICustomJsonConverter != null
            ? ICustomJsonConverter.convert(consumerRecord.value())
            : consumerRecord.value();

    kafkaMessageSubject.notifyObservers(consumerRecord.topic(), finalMessage);
  }
}
