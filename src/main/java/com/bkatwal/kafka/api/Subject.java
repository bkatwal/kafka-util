package com.bkatwal.kafka.api;

/** @author "Bikas Katwal" 17/03/19 */
public interface Subject {

  void registerObserver(String topic, Observer observer);

  void unregisterObserver(String topic, Observer observer);

  void notifyObservers(String topic, String message);
}
