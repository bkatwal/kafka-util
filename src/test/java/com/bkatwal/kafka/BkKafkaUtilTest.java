package com.bkatwal.kafka;

import com.bkatwal.kafka.impl.ConcreteObserver;
import com.bkatwal.kafka.impl.CustomAckMessageListener;
import com.bkatwal.kafka.impl.KafkaMessageSubject;
import com.bkatwal.kafka.util.KafkaConsumerUtil;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.kafka.test.rule.KafkaEmbedded;

/** @author "Bikas Katwal" 29/03/19 */
public class BkKafkaUtilTest {

  @ClassRule
  public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, 1, "test-topic");

  @AfterClass
  public static void close() {
    try {
      embeddedKafka.destroy();
    } catch (Exception e) {
      // do nothing
    }
  }

  /**
   * this test will give a demo of the usage of this utility project. Below code needs to be used in
   * the client that uses this project
   */
  @Test
  public void kafkaUtilTest() throws InterruptedException {
    Logger.getRootLogger().setLevel(Level.INFO);
    String broker = embeddedKafka.getBrokerAddresses()[0].toString();

    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", broker);
    props.put("group.id", "test-group");
    props.put("enable.auto.commit", false);
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    // this needs to be created based on you target database
    // in thi example it will just print on console
    SampleSinkService sampleSinkService = new SampleSinkService();

    ConcreteObserver concreteObserver = new ConcreteObserver(sampleSinkService);

    KafkaMessageSubject kafkaMessageSubject = new KafkaMessageSubject();
    kafkaMessageSubject.registerObserver("test-topic", concreteObserver);

    SampleCustomJsonConverter sampleCustomJsonConverter = new SampleCustomJsonConverter();
    CustomAckMessageListener customAckMessageListener =
        new CustomAckMessageListener(sampleCustomJsonConverter, kafkaMessageSubject);

    KafkaConsumerUtil.startOrCreateConsumers("test-topic", customAckMessageListener, 1, props);

    //waiting for consumer to start and partition assignment
    Thread.sleep(30000);
    sampleProducer(broker);

    //wait for consumer to finish consuming data
    Thread.sleep(10000);

    KafkaConsumerUtil.stopConsumer("test-topic");
  }

  private void sampleProducer(String bootstrapServer) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServer);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 2);
    props.put("linger.ms", 1);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);
    for (int i = 0; i < 100; i++)
      producer.send(
          new ProducerRecord<>("test-topic", Integer.toString(i), "{\"name\":\"bikas\"}"));

    producer.close();
  }
}
