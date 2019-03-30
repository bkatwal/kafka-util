package com.bkatwal.kafka;

import com.bkatwal.kafka.api.SinkService;
import lombok.extern.slf4j.Slf4j;

/** @author "Bikas Katwal" 30/03/19 */

/** A similar class needs to be created to save data to target DB */
@Slf4j
public class SampleSinkService implements SinkService {

  @Override
  public boolean update(String jsonData) {
    log.info("saving message to database: {}", jsonData);
    // some save logic
    return true;
  }

  @Override
  public boolean update(String targetName, String jsonData) {
    log.info("saving message to database: {}", jsonData);
    // some save logic
    return true;
  }
}
