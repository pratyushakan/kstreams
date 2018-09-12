package org.kafka.streamsample;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class JoinStreamSample {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joinSample");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		String productsTopic = "test1";
		String pricesTopic = "test2";
		String outputTopic = "test-output";
		 KTable<String, String> products = builder.table(productsTopic);
		    KTable<String, String> prices = builder.table(pricesTopic);

		    String storeName = "test1-test2";
		   /* products.join(prices,
		        (productValue, priceValue) -> productValue,
		        Materialized.as(storeName))
		        .toStream()
		        .to(outputTopic);*/

		    products.join(prices,
			        (productValue, priceValue) -> productValue + "," + priceValue,
			        Materialized.as(storeName))
			        .toStream()
			        .to(outputTopic);


		    KafkaStreams streams = new KafkaStreams(builder.build(), props);
		    streams.start();
	}
}
