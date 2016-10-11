/**
 * Created by cuixuan on 9/28/16.
 */
import api.Message;
import api.MessageConsumer;
import org.apache.kafka.clients.consumer.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;

import java.util.Set;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Collections;
import java.util.Properties;
import java.util.HashMap;
import java.util.Arrays;
import java.util.Collection;
import java.util.ArrayList;

public class KafkaMessageConsumer implements MessageConsumer {

    private String topic;
    private KafkaConsumer<String, String> consumer;
    private long timeout = 10000;
    private ConsumerRecords<String, String> records = null;
    private Set<TopicPartition> topicPartitions = null;
    private Iterator<TopicPartition> topicPartitionIt = null;
    private TopicPartition topicPartition = null;
    private List<ConsumerRecord<String, String>> recordsList = null;
    private int recordIndex = 0;
    private int recordsListSize = 0;

    private ConsumerRecord<String, String> getRecord() {

        ConsumerRecord<String, String> record = null;

        // there is no available records memory and it is needed to fetch more from the topic or topic list
        if (records == null || (!topicPartitionIt.hasNext() && recordIndex >= recordsListSize)) {
            records = consumer.poll(timeout);
            if (records == null) {
                System.out.println("No available records.");
                return null;
            }
            topicPartitions = records.partitions();
            topicPartitionIt = topicPartitions.iterator();
        }

        if (recordIndex >= recordsListSize) {
            topicPartition = topicPartitionIt.next();
            recordsList = records.records(topicPartition);
            recordsListSize = recordsList.size();
            recordIndex = 0;
        }

        record = recordsList.get(recordIndex);
        recordIndex ++;

        Map<TopicPartition, OffsetAndMetadata> offsets =
                Collections.singletonMap(topicPartition, new OffsetAndMetadata(record.offset() + 1));
        try {
            consumer.commitAsync(offsets, new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
                    System.out.println("Commit completed.");
                }
            });
        } catch (CommitFailedException e) {
            System.out.println("Commit failed.");
        }

        return record;
    }

    public KafkaMessageConsumer(Configuration conf, String topicOut) {

        topic = topicOut;
        Properties props = new Properties();
        Map<String, String> defaults = new HashMap<>();
        Map<String, String> overrides = new HashMap<>();

        defaults.put("zookeeper.connect", "localhost:2181");
        defaults.put("auto.offset.reset", "earliest");
        defaults.put("group.id", "group1");
        defaults.put("zookeeper.session.timeout.ms", "400");
        defaults.put("zookeeper.sync.time.ms", "200");
        defaults.put("auto.commit.intervals.ms", "1000");
        defaults.put("bootstrap.servers", "localhost:9092");
        defaults.put("enable.auto.commit", "false");
        defaults.put("session.timeout.ms", "30000");
        defaults.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        defaults.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        for (Iterator<Map.Entry<String, String>> conf_i = conf.iterator(); conf_i.hasNext(); ) {
            Map.Entry<String, String> entry = conf_i.next();
            overrides.put(entry.getKey(), entry.getValue());
        }

        props.putAll(defaults);
        props.putAll(overrides);

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    public Message receive () {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        ConsumerRecord<String, String> record = getRecord();

        if (record != null) {
            partitionId = record.partition();
            key = String.valueOf(record.key()).getBytes();
            value = String.valueOf(record.value()).getBytes();
            offset = record.offset();
            System.out.println(String.valueOf(offset));
        }

        Message msg = new Message(partitionId, key, value);
        return msg;
    }

    public Collection<Message> receive(int size) {

        long partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        int size_i = 0;
        Collection<Message> msgList = new ArrayList<Message>();
        int partitionNum = topicPartitions.size();

        System.out.println("Topic Partitions Number: " + String.valueOf(partitionNum));
        System.out.println("Record List Size: " + String.valueOf(recordsListSize));

        ConsumerRecord<String, String> record = null;
        while (size_i < size) {
            record = getRecord();
            if (record == null) {
                return msgList;
            }
            partitionId = record.partition();
            key = String.valueOf(record.key()).getBytes();
            value = String.valueOf(record.value()).getBytes();
            offset = record.offset();
            System.out.println(String.valueOf(offset));
            Message msg = new Message((int)partitionId, key, value);
            msgList.add(msg);
            size_i ++;
        }

        return msgList;
    }

    public void close () {
        consumer.close();
    }
}
