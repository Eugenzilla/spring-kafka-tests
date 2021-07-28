package io.eugenzilla.springkafkatests;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class TestProducer implements Closeable {

    private KafkaProducer<String, String> prod = getProducer();
    private String topic;

    public TestProducer(String topic) {
        this.topic = topic;
    }

    private KafkaProducer<String, String> getProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducer<>(props);
    }

    public void send (String key, String value) throws ExecutionException, InterruptedException {
        		prod
				.send(new ProducerRecord<String,String>(topic, key,value))
				.get();
    }

    @Override
    public void close() throws IOException {
        prod.close();
    }
}
