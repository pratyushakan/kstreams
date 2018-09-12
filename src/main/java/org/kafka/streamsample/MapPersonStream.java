package org.kafka.streamsample;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.lang3.StringUtils;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

public class MapPersonStream {	

	public static void main(String[] args) {

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-testPersonContact");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		StreamsBuilder builder = new StreamsBuilder();
		String test1Topic = "test1-sqlserver-jdbc-cdc-all-dbo-person";
		String test2Topic = "test1-sqlserver-jdbc-cdc-all-dbo-contact";
		//String test3Topic = "test1-sqlserver-jdbc-cdc-all-dbo-address";

		String outputTopic = "test1";
		KStream<String, String> testStream1 = builder.stream(test1Topic);
		KStream<String, String> testStream2 = builder.stream(test2Topic);
		//KStream<String, String> testStream3 = builder.stream(test3Topic);
		String storeName = "test4PersonContact";
		
		KStream<String, String> transformedStream1 = testStream1.mapValues(value -> {
			value = value.replaceAll("\\{", "");
			value = value.replaceAll("}", "");
			String[] parts = value.split(",");
			List<String> result = new ArrayList<String>();
			for(String part : parts) {
				if(!part.contains("__$"))
				result.add(part);
			}			
			return StringUtils.join(result, ',');
			});
		
		KStream<String, String> transformedStream2 = testStream2.mapValues(value -> {
			value = value.replaceAll("\\{", "");
			value = value.replaceAll("}", "");
			String[] parts = value.split(",");
			List<String> result = new ArrayList<String>();
			for(String part : parts) {
				if(!part.contains("__$"))
				result.add(part);
			}
			
			return StringUtils.join(result, ',');
			});
		
		/*KStream<String, String> transformedStream3 = testStream3.mapValues(value -> {
			value = value.replaceAll("\\{", "");
			value = value.replaceAll("}", "");
			String[] parts = value.split(",");
			List<String> result = new ArrayList<String>();
			for(String part : parts) {
				if(!part.contains("__$"))
				result.add(part);
			}
			
			return StringUtils.join(result, ',');
			});*/
		
		
		transformedStream1.to(outputTopic);
		transformedStream2.to(outputTopic);
		//transformedStream3.to(outputTopic);
		
		KafkaStreams streams = new KafkaStreams(builder.build(), props);
		streams.start();
	}
}
