/**
 * Created by cuixuan on 9/28/16.
 */
import api.ProducerMessage;
import api.MessageProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.hadoop.conf.Configuration;

import java.util.HashSet;
import java.util.Properties;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;

public class KafkaMessageProducer implements MessageProducer {

    private String topic;
    private Producer<String, String> producer;
    private HashSet<String> propsSet = new HashSet<>();

    public KafkaMessageProducer(Configuration conf, String topicOut) {
        if (topicOut == null)
            throw new IllegalArgumentException("Topic cannot be null");

        topic = topicOut;
        Properties props = new Properties();
        Map<String, String> defaults = new HashMap<>();
        Map<String, String> overrides = new HashMap<>();

        defaults.put("bootstrap.servers", "localhost:9092");
        defaults.put("acks", "all");
        defaults.put("retries", "0");
        defaults.put("batch.size", "16384");
        defaults.put("linger.ms", "1");
        defaults.put("buffer.memory", "33554432");
        defaults.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        defaults.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        for (Iterator<Map.Entry<String, String>> conf_i = conf.iterator(); conf_i.hasNext(); ) {
            Map.Entry<String, String> entry = conf_i.next();
            overrides.put(entry.getKey(), entry.getValue());
        }

        props.putAll(defaults);
        props.putAll(overrides);

        producer = new KafkaProducer<String, String>(props);
    }

    public void sendAsync (ProducerMessage msg) {

        String keyStr = null;
        String valueStr = null;
        ProducerRecord producerRecord;

        if (msg.key() != null) {
            keyStr = new String(msg.key());
        }

        if (msg.value() != null) {
            valueStr = new String(msg.value());
        }


        producerRecord= new ProducerRecord(topic, msg.partitionId(), msg.timestamp(),
                keyStr, valueStr);


        producer.send(producerRecord);
    }

    public void close () {
        producer.close();
    }

}

