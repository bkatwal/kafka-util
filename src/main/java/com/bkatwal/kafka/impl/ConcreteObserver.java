package com.bkatwal.kafka.impl;

import com.bkatwal.kafka.api.Observer;
import com.bkatwal.kafka.api.SinkService;
import lombok.AllArgsConstructor;

/** @author "Bikas Katwal" 19/03/19 */
@AllArgsConstructor
public class ConcreteObserver implements Observer {

  private SinkService sinkService;

  @Override
  public void update(String topic, String message) {

    sinkService.update(topic, message);
  }
}
