package org.kafka.streamsample;

import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

public class JoinStream {

	public static void main(String[] args) {
		
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-joinSampleCDC8");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		
		StreamsBuilder builder = new StreamsBuilder();
		String productsTopic = "test1-sqlserver-jdbc-cdc-all-dbo-person";
		String pricesTopic = "test1-sqlserver-jdbc-cdc-all-dbo-contact";
		String outputTopic = "test2";
		 KTable<String, String> products = builder.table(productsTopic);
		    KTable<String, String> prices = builder.table(pricesTopic);

		    String storeName = "person-contact8";
		    products.join(prices,
		        (productValue, priceValue) -> {
		        	String result = productValue + "," + priceValue;	
		        	result = result.replace("}","");
		        	result = result.replace("\\{","");
		        	String[] resultParts = result.split(",");
		        	StringBuffer joinValue = new StringBuffer();
		        	for(String part : resultParts) {
		        		String[] temp = part.split(":");
		        		if(!temp[0].contains("start_lsn")){
		        			if(joinValue.length() > 0)
		        				joinValue.append(",");
		        			joinValue.append(part);
		        		}
		        	}
		        	return joinValue.toString();
		        	},
		        Materialized.as(storeName))
		        .toStream()
		        .to(outputTopic);

		    KafkaStreams streams = new KafkaStreams(builder.build(), props);
		    streams.start();
	}
}
