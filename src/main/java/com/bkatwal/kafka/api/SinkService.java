package com.bkatwal.kafka.api;

/** @author "Bikas Katwal" */
public interface SinkService {

  boolean update(String jsonData);

  boolean update(String targetName, String jsonData);
}
