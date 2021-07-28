package io.eugenzilla.springkafkatests;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@SpringBootApplication
public class SpringKafkaTestsApplication {

	public static void main(String[] args) throws ExecutionException, InterruptedException {

		SpringApplication.run(SpringKafkaTestsApplication.class, args);

		String topic = "spring-kafka-tests";
		TestProducer testProducer = new TestProducer(topic);

		new Thread( () -> {
			IntStream.iterate(1, n -> n + 1)
					.limit(100)
					.forEach(i -> {
						try {
							testProducer.send(Integer.toString(i), "Hello from TestProducer!");
							TimeUnit.SECONDS.sleep(5);
						} catch (Exception e) {
							e.printStackTrace();
						}
					});
		}).start();

		var consumer = new TestConsumer(topic);
		consumer.consume((record) -> {
			System.out.println("Got key: " + record.key() + ", value: " + record.value());
		});

		TimeUnit.MINUTES.sleep(10);

		try {
			testProducer.close();
			consumer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
