#### This project demostrates, how you can start/stop a kafka consumer programmatically as and when needed.

#### Below test class demostrates how you can use this class:

https://github.com/bkatwal/kafka-util/blob/master/src/test/java/com/bkatwal/kafka/BkKafkaUtilTest.java

## Overview
Each consumer you create needs to be mapped to the topic and stored in a HashMap, that is later used to  start/stop consumer.

Use below method to start/create a consumer:
```
KafkaConsumerUtil.startOrCreateConsumers(
final String topic, 
final Object messageListener, 
final int concurrency, 
final Map<String, Object> consumerProperties){
.......
.......
}
```
Use below method to stop a consumer:
```
KafkaConsumerUtil.stopConsumer(final String topic);
```
Follow below steps to create a new consumer:

1. Provide your own implementation of `SinkService`, to save kafka topic data to your target database.
Example: 
```
// this needs to be your implementation based on the database you want the data to be saved
// below will just print on console
SampleSinkService sampleSinkService = new SampleSinkService();
```
2. Create an object of `ConcreteObserver` and pass object of SinkService you created in previous step. 
Example:
```
// create an object of ConcreteObserver with your implementation of SinkService interface
ConcreteObserver concreteObserver = new ConcreteObserver(sampleSinkService);
```
3. Create an object of KafkaMessageSubject and register the observer.
Example:
```
// create and object of KafkaMessageSubject and register you observer created in previous step.
KafkaMessageSubject kafkaMessageSubject = new KafkaMessageSubject();
kafkaMessageSubject.registerObserver("test-topic", concreteObserver);
```
Note: pass topic name while registering, this will be later used to start/stop consumer.
4. Create your own Json converter for your business logic if you want some processing to be done on topic output json before saving it to database by implementing interface ICustomJsonConverter.
Then create an object of `CustomAckMessageListener` and pass this converter as constructor param.
If you do not need json message conversion use 1 argument constructor and pass only the `KafkaMessageSubject`
```
// create your own Json converter based on your business logic implementation, by implementing
// interface ICustomJsonConverter
// if no Json converter passed to the Message Listener, use different constructor
SampleCustomJsonConverter sampleCustomJsonConverter = new SampleCustomJsonConverter();
CustomAckMessageListener customAckMessageListener =
new CustomAckMessageListener(sampleCustomJsonConverter, kafkaMessageSubject);
```
You can use either `CustomAckMessageListener` or `CustomMessageListener` message listner, based on whether you have auto commit enabled or not.
5. Create consumer properties `Map<String,Object>` say `props`.
6. Create consumer using below method:
```
//start the consumer
KafkaConsumerUtil.startOrCreateConsumers("test-topic", customAckMessageListener, 1, props);
```
In above call, we are creating 1 consumer for topic `test-topic`
