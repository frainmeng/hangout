package com.ctrip.ops.sysdev.outputs;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.ctrip.ops.sysdev.baseplugin.BaseOutput;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import lombok.extern.log4j.Log4j;
import org.json.simple.JSONValue;

@Log4j
public class Kafka extends BaseOutput {

    private Producer producer;
    private String topic;
    private Properties props;

    public Kafka(Map config) {
        super(config);
    }

    public Kafka(Map config, List<BaseOutput> errorOutputProcessors) {
        super(config,errorOutputProcessors);
    }
    protected void prepare() {
        if (!this.config.containsKey("topic")) {
            log.error("topic must be included in config");
            System.exit(1);
        }
        this.topic = (String) this.config.get("topic");

        props = new Properties();

        HashMap<String, String> producerSettings = (HashMap<String, String>) this.config.get("producer_settings");

        if (producerSettings != null) {
            producerSettings.entrySet().stream().forEach(entry -> {
                String k = entry.getKey();
                String v = entry.getValue();
                props.put(k, v);
            });
        } else {
            log.error("producer_settings must be included in config");
            System.exit(1);
        }

        if (props.get("bootstrap.servers") == null) {
            log.error("bootstrap.servers must be included in producer_settings");
            System.exit(1);
        }

        if (props.get("key.serializer") == null) {
            props.put("key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        }
        if (props.get("value.serializer") == null) {
            props.put("value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer");
        }
        producer = new KafkaProducer<>(props);
    }

    protected void emit(Map event) {
        producer.send(new ProducerRecord<String, String>(topic, JSONValue.toJSONString(event)));
    }

    public void shutdown() {
        log.info("close producer and then shutdown");
        producer.close();
    }
}
