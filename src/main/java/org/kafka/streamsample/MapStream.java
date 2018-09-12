package org.kafka.streamsample;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class MapStream {

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-test1Sample1");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		String test1Topic = "test1";

		String outputTopic = "test1";
		KStream<String, String> testStream = builder.stream(test1Topic);
		String storeName = "test1";
		
		KStream<String, String> transformedStream = testStream.mapValues(value -> value.toUpperCase());
		transformedStream.to(outputTopic);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}
