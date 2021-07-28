package io.eugenzilla.springkafkatests;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Properties;

@SpringBootApplication
public class SpringKafkaTestsApplication {

	public static void main(String[] args) {

		//SpringApplication.run(SpringKafkaTestsApplication.class, args);

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.setProperty(ProducerConfig.CLIENT_ID_CONFIG, "clientId");
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		KafkaProducer<String, String> prod = new KafkaProducer<>(props);
		String topic = "spring-kafka-tests";
		prod.send(new ProducerRecord<String,String>(topic, "Hello from JVM!!"));
	}
}
