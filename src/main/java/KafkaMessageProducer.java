/**
 * Created by cuixuan on 9/28/16.
 */
import org.apache.kafka.clients.producer.*;

import org.apache.hadoop.conf.Configuration;
import java.util.Properties;

public class KafkaMessageProducer {

    private String topic;
    private Producer<String, String> producer;

    public KafkaMessageProducer(Configuration conf, String topicOut) {

        Properties props = new Properties();

        String bootstrap = conf.get("bootstrap.servers");
        String acks = conf.get("acks");
        String retries = conf.get("retries");
        String batchSize = conf.get("batch.size");
        String lingerMs = conf.get("linger.ms");
        String bufferMemory = conf.get("buffer.memory");
        String keySerializer = conf.get("key.serializer");
        String valueSerializer = conf.get("value.serializer");

        props.put("bootstrap.servers", bootstrap); //"localhost:9092");
        props.put("acks", acks);//"all");
        props.put("retries", retries);//0);
        props.put("batch.size", batchSize);//16384);
        props.put("linger.ms", lingerMs);//1);
        props.put("buffer.memory", bufferMemory);//33554432);
        props.put("key.serializer", keySerializer);//"org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", valueSerializer);//"org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);

        topic = topicOut;
    }

    void sendAsync (Message msg) {
        String keyStr = null;
        if (msg.key != null) {
            keyStr = new String(msg.key);
        }
        String valueStr = null;
        if (msg.value != null) {
            valueStr = new String(msg.value);
        }
        ProducerRecord producerRecord = new ProducerRecord(topic, msg.partitionId,
                keyStr, valueStr);

//        ProducerRecord producerRecord = new ProducerRecord(topic, msg.partitionId,
//                msg.key, msg.value);
        producer.send(producerRecord);
        //producer.flush();
    }

    void close () {
        producer.close();
    }

}

