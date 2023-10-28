package com.abhirockzz;

import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
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
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

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
    public void shouldIncludeValueWithLengthGreaterThanFive() {

        topology = App.retainWordsLongerThan5Letters();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("foo", "barrrrr");
        assertEquals(outputTopic.readValue(), "barrrrr");
        assertTrue(outputTopic.isEmpty());

        inputTopic.pipeInput("foo", "bar");
        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void testFlatMap() {
        topology = App.flatMap();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());

        inputTopic.pipeInput("random", "foo,bar,baz");
        inputTopic.pipeInput("hello", "world,universe");
        inputTopic.pipeInput("hi", "there");

        assertEquals(outputTopic.getQueueSize(), 6L);

        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("random", "foo"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("random", "bar"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("random", "baz"));

        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("hello", "world"));
        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("hello", "universe"));

        assertEquals(outputTopic.readKeyValue(), new KeyValue<>("hi", "there"));

        assertTrue(outputTopic.isEmpty());
    }

    @Test
    public void shouldGroupRecordsAndProduceValidCount() {
        topology = App.count();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        TestOutputTopic<String, Long> ot = td.createOutputTopic(App.OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.Long().deserializer());

        inputTopic.pipeInput("key1", "value1");
        inputTopic.pipeInput("key1", "value2");
        inputTopic.pipeInput("key2", "value3");
        inputTopic.pipeInput("key3", "value4");
        inputTopic.pipeInput("key2", "value5");

        assertEquals(ot.readKeyValue(), new KeyValue<String, Long>("key1", 1L));
        assertEquals(ot.readKeyValue(), new KeyValue<String, Long>("key1", 2L));
        assertEquals(ot.readKeyValue(), new KeyValue<String, Long>("key2", 1L));
        assertEquals(ot.readKeyValue(), new KeyValue<String, Long>("key3", 1L));
        assertEquals(ot.readKeyValue(), new KeyValue<String, Long>("key2", 2L));

        assertTrue(ot.isEmpty());
    }

    @Test
    public void shouldProduceValidCountInStateStore() {
        topology = App.countWithStateStore();
        td = new TopologyTestDriver(topology, config);

        inputTopic = td.createInputTopic(App.INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        
        KeyValueStore<String, Long> countStore = td.getKeyValueStore("count-store");

        inputTopic.pipeInput("key1", "value1");
        assertEquals(countStore.get("key1"), 1L);

        inputTopic.pipeInput("key1", "value2");
        assertEquals(countStore.get("key1"), 2L);

        inputTopic.pipeInput("key2", "value3");
        assertEquals(countStore.get("key2"), 1L);

        inputTopic.pipeInput("key3", "value4");
        assertEquals(countStore.get("key3"), 1L);

        inputTopic.pipeInput("key2", "value5");
        assertEquals(countStore.get("key2"), 2L);
    }
}
