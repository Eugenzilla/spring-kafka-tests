package io.eugenzilla.springkafkatests;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class TestConsumer implements Closeable {

    private String topic;
    private KafkaConsumer<String, String> consumer;

    public TestConsumer(String topic) {
        this.topic = topic;
        consumer = getConsumer(topic);
    }

    private KafkaConsumer<String, String> getConsumer(String topic) {
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "groupId");
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        var consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    public void consume(Consumer<ConsumerRecord<String, String>> recordConsumer){
        new Thread( () -> {
           while(true) {
               var records = consumer.poll(Duration.ofSeconds(1));
               records.forEach(rec -> {
                   recordConsumer.accept(rec);
               });
           }
        }).start();
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
