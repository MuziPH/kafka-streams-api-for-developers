package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

public class GreetingsTopology {
    public static String GREETINGS = "greetings";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    // Holds the frame of the application, source-process-sink
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        // Source processor - Read form a provided GREETINGS topic
        KStream<String, String> greetingsStream = streamsBuilder
                .stream(GREETINGS, Consumed.with(Serdes.String(), Serdes.String()));

        // Print the Original stream
        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        // Processor the streaming records
        KStream<String, String> modifiedStream = greetingsStream
                .filter((key, greeting) -> greeting.length() > 5)
                .mapValues((readOnlyKey, value) -> value.toUpperCase());
        // Print the transformed stream
        modifiedStream
                .print(Printed.<String, String>toSysOut().withLabel("modifiedStream"));

        // Sink Processor - Publish to GREETINGS_UPPERCASE topic
        modifiedStream
                .to(GREETINGS_UPPERCASE, Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }
}
