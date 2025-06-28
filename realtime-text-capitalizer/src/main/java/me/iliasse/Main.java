package me.iliasse;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class Main {
    public static void main(String[] args) {


        Properties props = new Properties();

        // Configures l'application
        props.put("application.id", "realtime-text-capitalizer");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());


        // Builds a kafka stream
        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> sourceStream = builder.stream("topic1");


        // Deserialize and capitalize the received text received from the "input-topic" stream
        KStream<String, String> upperCaseStream = sourceStream.mapValues(value -> {
            System.out.println(value);
            return value.toUpperCase();
        });


        //serialize and send data to the output-topic
        upperCaseStream.to("topic2");


        // Instantiate the kafka streams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        //launch the application
        streams.start();


        // This will allow us to shutdown the application gracefully (after clearning ressources ...)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}