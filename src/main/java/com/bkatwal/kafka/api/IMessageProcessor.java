package com.bkatwal.kafka.api;

/** @author "Bikas Katwal" 20/02/19 */
public interface IMessageProcessor {

  void process(String key, String json);
}
