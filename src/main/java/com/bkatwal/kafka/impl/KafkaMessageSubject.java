package com.bkatwal.kafka.impl;

import com.bkatwal.kafka.api.Observer;
import com.bkatwal.kafka.api.Subject;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** @author "Bikas Katwal" 19/03/19 */
public class KafkaMessageSubject implements Subject {

  private Map<String, Set<Observer>> topicToSubscribersMap;

  public KafkaMessageSubject() {
    this.topicToSubscribersMap = new LinkedHashMap<>();
  }

  /**
   * Observer will look for updates from given topic Make sure observes are added in order as per
   * application requirement
   *
   * @param topic kafka topic
   * @param observer topic observer which will update target
   */
  @Override
  public void registerObserver(String topic, Observer observer) {
    if (observer == null) {
      throw new NullPointerException("Null Observer");
    }

    if (topicToSubscribersMap.containsKey(topic)) {
      topicToSubscribersMap.get(topic).add(observer);
    } else {
      topicToSubscribersMap.put(topic, new LinkedHashSet<>());
      topicToSubscribersMap.get(topic).add(observer);
    }
  }

  @Override
  public void unregisterObserver(String topic, Observer observer) {
    topicToSubscribersMap.get(topic).remove(observer);
  }

  /**
   * Updates the targets/observers, If any update fails, subsequent updates to other observers will
   * fail
   *
   * @param topic
   * @param message
   */
  @Override
  public void notifyObservers(String topic, String message) {
    Set<Observer> observers = topicToSubscribersMap.get(topic);

    observers.forEach(obj -> obj.update(topic, message));
  }
}
