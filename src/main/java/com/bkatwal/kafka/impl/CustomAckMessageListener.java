package com.bkatwal.kafka.impl;

import com.bkatwal.kafka.api.IMessageProcessor;
import lombok.AllArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

/** @author "Bikas Katwal" 13/03/19 */
@AllArgsConstructor
public class CustomAckMessageListener implements AcknowledgingMessageListener<String, String> {

  private IMessageProcessor messageProcessor;

  @Override
  public void onMessage(
      ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {

    messageProcessor.process(consumerRecord.key(), consumerRecord.value());

    acknowledgment.acknowledge();
  }
}
