package me.iliasse;


import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Date;
import java.util.Properties;

public class Main {
    public static void main(String[] args) {

        Long timestamp = new Date().getTime();

        String inputStream = "weather-data";
        String outputStream = "station-averages";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-processing-"+timestamp);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();


        //input stream (reads data from the weather-data topic)
        KStream<String, String> input = builder.stream(inputStream);


        //filtering (only records with temperature > 30)
        KStream<String, String> filtered = input.filter((s, s2) -> {
            try{
                return Double.parseDouble(s2.split(",")[1]) > 30;
            }
            catch(Exception ex){
                throw new RuntimeException(ex);
            }
        });

        //converting temperature from C° to fahrenheit
        KStream<String, String> converted = filtered.mapValues((val) -> {
            try{
                String[] parts = val.split(",");
                Double temp = Double.parseDouble(parts[1]);
                temp = (temp * 9d/5) + 32;
                parts[1] = temp.toString();

                return String.join(",", parts);
            }
            catch(Exception ex){
                throw new RuntimeException();
            }
        });

        //grouping data by station name
        KGroupedStream<String, String> grouped = converted.selectKey((s, s2) -> {
            try{
                return s2.split(",")[0];
            }
            catch(Exception ex){
                throw new RuntimeException(ex);
            }
        }).groupByKey(Grouped.with(Serdes.String(), Serdes.String()));

        //calculates the temperature's and humidity's sums of each station
        KTable<String, String> aggregated = grouped.aggregate(() -> "0.0,0.0,0",
            (key, val, agg) -> {
                try{
                    String values[] = val.split(",");
                    String aggParts[] = agg.split(",");

                    Double sumTemp = Double.parseDouble(aggParts[0]);
                    Double sumHum = Double.parseDouble(aggParts[1]);
                    Long count = Long.parseLong(aggParts[2]);

                    sumTemp += Double.parseDouble(values[1]);
                    sumHum += Double.parseDouble(values[2]);
                    count += 1;

                    return sumTemp+","+sumHum+","+count;
                }
                catch(Exception ex){
                    throw new RuntimeException(ex);
                }
        }, Materialized.with(Serdes.String(), Serdes.String()));

        //calculates the avearges and publish data to the "station-averages" topic
        aggregated.toStream().mapValues((s, s2) -> {
            try{
                String[] parts = s2.split(",");
                Double avgTemp = Double.parseDouble(parts[0]) / Long.parseLong(parts[2]);
                Double avgHum = Double.parseDouble(parts[1]) / Long.parseLong(parts[2]);

                return s + ": Température Moyenne = " + avgTemp + "°F - Humidité Moyenne = " + avgHum + "%";
            }
            catch(Exception ex){
                throw new RuntimeException(ex);
            }
        }).to(outputStream, Produced.with(Serdes.String(), Serdes.String()));


        //Initialize and starts KafkaStreams application
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        //This line allows KafkaStream to terminates its execution gracefully (clearing ressources ...)
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}