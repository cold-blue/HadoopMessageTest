/**
 * Created by cuixuan on 9/28/16.
 */
import org.apache.kafka.clients.producer.*;

import org.apache.hadoop.conf.Configuration;
import java.util.Properties;

public class MessageProducer {

    private String topic;
    private Producer<String, String> producer;

    public MessageProducer (Configuration conf, String topicOut) {

        Properties props = new Properties();

        String bootstrap = conf.get("bootstrap.servers");
        String acks = conf.get("acks");
        String retries = conf.get("retries");
        String batchSize = conf.get("batch.size");
        String lingerMs = conf.get("linger.ms");
        String ufferMemory = conf.get("uffer.memory");
        String keySerializer = conf.get("key.serializer");
        String valueSerializer = conf.get("value.serializer");

        props.put("bootstrap.servers", bootstrap); //"localhost:9092");
        props.put("acks", acks);//"all");
        props.put("retries", retries);//0);
        props.put("batch.size", batchSize);//16384);
        props.put("linger.ms", lingerMs);//1);
        props.put("buffer.memory", ufferMemory);//33554432);
        props.put("key.serializer", keySerializer);//"org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerializer);//"org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

        topic = topicOut;
    }

    void sendAsync (Message msg) {
        ProducerRecord producerRecord = new ProducerRecord(topic, msg.partitionId, msg.key, msg.value);
        producer.send(producerRecord);
    }

    void close () {
        producer.close();
    }

}

