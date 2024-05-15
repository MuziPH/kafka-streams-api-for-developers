package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {
    public static String GREETINGS = "greetings";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";
    public static String GREETINGS_SPANISH = "greetings_spanish";

    // Holds the frame of the application, source-process-sink
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Source processor - Read form a provided GREETINGS topic
        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        // Print the Original stream
        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        // Source Processor
        KStream<String, String> greetingsSpanishStream = streamsBuilder
                .stream(GREETINGS_SPANISH, Consumed.with(Serdes.String(), Serdes.String()));

        // Print greetingsSpanishStream
        greetingsSpanishStream
                .print(Printed.<String, String>toSysOut().withLabel("spanishStream"));

        // Merge greetingsStream with greetingsSpanishStream
        // Consume from two topics into one
        KStream<String, String> mergedStream = greetingsStream.merge(greetingsSpanishStream);

        // Print the Merged stream
        mergedStream
                .print(Printed.<String, String>toSysOut().withLabel("mergedStream"));

        // Processor the streaming records
        KStream<String, String> modifiedStream = mergedStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase());
/*
        KStream<String, String> modifiedStream = greetingsStream
                .filter((key, value) -> value.length() > 5)
                .peek((key, value) -> {
                    log.info("After filter : key: {}, value:{}", key, value);
                })
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .peek((key, value) -> {
                    log.info("after filter : key : {} : value : {}", key, value);
                }).flatMapValues((key, value) -> {
                    List<String> newValues = Arrays.asList(value.split(""));
                    return newValues
                            .stream()
                            .map(String::toUpperCase)
                            .collect(Collectors.toList());
                });
*/
/*
        KStream<String, String> modifiedStream = greetingsStream
                //.filter((key, greeting) -> greeting.length() > 5)
                //.mapValues((readOnlyKey, value) -> value.toUpperCase());
                //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))
                .flatMap((key, value) -> {
                    List<String> newValues = Arrays.asList(value.split(""));
                    return newValues
                            .stream()
                            .map(val -> KeyValue.pair(key, val.toUpperCase()))
                            .collect(Collectors.toList());
                });
        // flatMapValues exposes only values, keys are readonly
        KStream<String, String> modifiedStream = greetingsStream
                .flatMapValues((key, value) -> {
                    // create flattened structure
                    List<String> newValues = Arrays.asList(value.split(""));
                    return newValues
                            .stream()
                            .map(String::toUpperCase)
                            .collect(Collectors.toList());
                });
*/

        // Print the transformed stream
        modifiedStream
                .print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        // Sink Processor - Publish to GREETINGS_UPPERCASE topic
        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
