package com.bkatwal.kafka;

import com.bkatwal.kafka.api.ICustomJsonConverter;

/** @author "Bikas Katwal" 30/03/19 */
public class SampleCustomJsonConverter implements ICustomJsonConverter {

  @Override
  public String convert(String json) {
    // add some custom logic here to covert to target json that needs to be saved to target DB
    // this will be you business logic
    return json;
  }
}
