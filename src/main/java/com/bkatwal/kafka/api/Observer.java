package com.bkatwal.kafka.api;

/** @author "Bikas Katwal" 19/03/19 */
public interface Observer {

  void update(String topic, String message);
}
