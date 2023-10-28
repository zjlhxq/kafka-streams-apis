package com.abhirockzz;


import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;


import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 * @author abhishekgupta
 */
public class AppTest {

    private TopologyTestDriver td;

    private Topology topology;
    private final Properties config;

    public AppTest() {

        config = new Properties();
        config.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, App.APP_ID);
        config.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "foo:1234");
        config.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    }

    @AfterEach
    public void tearDown() {
        td.close();
    }

    @Test
    public void aggregationShouldProduceValidCount() {

        topology = App.streamAggregation();
        td = new TopologyTestDriver(topology, config);

        TestInputTopic<String, String> inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Integer> outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Integer().deserializer());

        inputTopic.pipeInput("key1", "value1");
        assertEquals(outputTopic.readKeyValue(), new KeyValue<String, Integer>("key1", 1));

        inputTopic.pipeInput("key1", "value2");
        assertEquals(outputTopic.readKeyValue(), new KeyValue<String, Integer>("key1", 2));

        inputTopic.pipeInput("key2", "value3");
        assertEquals(outputTopic.readKeyValue(), new KeyValue<String, Integer>("key2", 1));

        inputTopic.pipeInput("key3", "value4");
        assertEquals(outputTopic.readKeyValue(), new KeyValue<String, Integer>("key3", 1));

        inputTopic.pipeInput("key2", "value5");
        assertEquals(outputTopic.readKeyValue(), new KeyValue<String, Integer>("key2", 2));

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldProduceMax() {
        
        topology = App.maxWithReduce();
        td = new TopologyTestDriver(topology, config);
        
        
        TestInputTopic<String, Long> inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.Long().serializer());
        TestOutputTopic<String, Long> outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());
        
        String key = "product1";
        
        inputTopic.pipeInput(key, 5L);
        assertEquals(outputTopic.readValue(), 5L);

        inputTopic.pipeInput(key, 6L);
        assertEquals(outputTopic.readValue(), 6L);

        inputTopic.pipeInput(key, 10L);
        assertEquals(outputTopic.readValue(), 10L);

        inputTopic.pipeInput(key, 2L);
        assertEquals(outputTopic.readValue(), 10L);

        inputTopic.pipeInput(key, 5L);
        assertEquals(outputTopic.readValue(), 10L);

        assertTrue(outputTopic.isEmpty());

    }
}
