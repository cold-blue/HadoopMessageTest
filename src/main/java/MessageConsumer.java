/**
 * Created by cuixuan on 9/28/16.
 */
import org.apache.kafka.clients.consumer.*;

import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class MessageConsumer {

    private String topic;
    private KafkaConsumer<String, String> consumer;

    public MessageConsumer (Configuration conf, String topicOut) {

        topic = topicOut;
        Properties props = new Properties();

        //String zookeeperConnect = conf.get("zookeeper.connect");
        String autoOffsetReset = conf.get("auto.offset.reset");
        String groupId = conf.get("group.id");
        //String zookeeperSessionTimeoutMs = conf.get("zookeeper.session.timeout.ms");
        //String zookeeperSyncTimeMs = conf.get("zookeeper.sync.time.ms");
        //String autoCommitIntervalsMs = conf.get("auto.commit.intervals.ms");
        String bootstrapServers = conf.get("bootstrap.servers");
        String enableAutoCommit = conf.get("enable.auto.commit");
        String sessionTimeoutMs = conf.get("session.timeout.ms");
        String keyDeserializer = conf.get("key.deserializer");
        String valueDeserializer = conf.get("value.deserializer");

        //props.put("zookeeper.connect", zookeeperConnect); //"localhost:2181");
        props.put("auto.offset.reset", autoOffsetReset);//"earliest");
        props.put("group.id", groupId);//"group1");
        //props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);// "400");
        //props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);// "200");
        //props.put("auto.commit.intervals.ms", autoCommitIntervalsMs); // "1000");
        props.put("bootstrap.servers", bootstrapServers);// "localhost:9092");
        props.put("enable.auto.commit", enableAutoCommit);// "true"); //close auto commit
        props.put("session.timeout.ms", sessionTimeoutMs);// "30000");
        props.put("key.deserializer", keyDeserializer);//"org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", valueDeserializer);// "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);

    }

    Message receive () {

        long partitionId = 0;
        byte[] key = null;
        byte[] value = null;

        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(1);


        for (ConsumerRecord<String, String> record : records) {
            partitionId = record.partition();
            key = record.key().getBytes();
            value = record.value().getBytes();
        }
        Message msg = new Message((int)partitionId, key, value);
        return msg;
    }

    Collection<Message> receive(int size) {

        long partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        Collection<Message> msgList = new ArrayList<Message>();

        consumer.subscribe(Arrays.asList(topic));
        ConsumerRecords<String, String> records = consumer.poll(1);

        for (ConsumerRecord<String, String> record : records) {
            partitionId = record.partition();
            key = record.key().getBytes();
            value = record.value().getBytes();
            Message msg = new Message((int)partitionId, key, value);
            msgList.add(msg);
        }

        return msgList;
    }

    void close () {
        consumer.close();
    }
}
