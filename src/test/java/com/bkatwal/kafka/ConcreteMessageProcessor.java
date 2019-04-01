package com.bkatwal.kafka;

import com.bkatwal.kafka.api.IMessageProcessor;
import lombok.extern.slf4j.Slf4j;

/** @author "Bikas Katwal" 30/03/19 */
@Slf4j
public class ConcreteMessageProcessor implements IMessageProcessor {

  @Override
  public void process(String key, String value) {
    // logic to process message and save to target
    log.info("processed message and saved to DB: {}", value);
  }
}
