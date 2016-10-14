/**
 * Created by cuixuan on 9/28/16.
 */
import api.ConsumerMessage;
import api.MessageConsumer;
import org.apache.kafka.clients.consumer.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;

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
//    private Set<TopicPartition> topicPartitions = null;
//    private Iterator<TopicPartition> topicPartitionIt = null;
//    private TopicPartition topicPartition = null;
//    private List<ConsumerRecord<String, String>> recordsList = null;
//    private int recordIndex = 0;
//    private int recordsListSize = 0;
    private int recordsSize = 0;
    private Iterator<ConsumerRecord<String, String>> recordIt = null;

//    private ConsumerRecord<String, String> getRecord() {
//
//        ConsumerRecord<String, String> record = null;
//
//        // there is no available records memory and it is needed to fetch more from the topic or topic list
//        if (records == null || (!topicPartitionIt.hasNext() && recordIndex >= recordsListSize)) {
//            records = consumer.poll(timeout);
//            if (records == null) {
//                System.out.println("No available records.");
//                return null;
//            }
//            topicPartitions = records.partitions();
//            topicPartitionIt = topicPartitions.iterator();
//        }
//
//
//        if (recordIndex >= recordsListSize) {
//            topicPartition = topicPartitionIt.next();
//            recordsList = records.records(topicPartition);
//            recordsListSize = recordsList.size();
//            recordIndex = 0;
//        }
//
//        record = recordsList.get(recordIndex);
//        recordIndex ++;
//
//        Map<TopicPartition, OffsetAndMetadata> offsets =
//                Collections.singletonMap(topicPartition, new OffsetAndMetadata(record.offset() + 1));
//        try {
//            consumer.commitAsync(offsets, new OffsetCommitCallback() {
//                @Override
//                public void onComplete(Map<TopicPartition, OffsetAndMetadata> map, Exception e) {
//                    System.out.println("Commit completed.");
//                }
//            });
//        } catch (CommitFailedException e) {
//            System.out.println("Commit failed.");
//        }
//
//        return record;
//    }

    private ConsumerRecord<String, String> getRecord() {

        ConsumerRecord<String, String> record = null;
        int recordPartition = 0;
        TopicPartition topicPartition = null;

        // there is no available records memory and it is needed to fetch more from the topic or topic list
        if (records == null || !recordIt.hasNext()) {
            records = consumer.poll(timeout);
            if (records == null) {
                System.out.println("No available records.");
                return null;
            }
            recordsSize = records.count();
            recordIt = records.iterator();
        }

        record = recordIt.next();
        recordPartition = record.partition();
        topicPartition = new TopicPartition(topic, recordPartition);

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

    public ConsumerMessage receive () {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        long timestamp;
        TimestampType timestampType;
        long checksum;
        int serializedKeySize;
        int serializedValueSize;
        ConsumerRecord<String, String> record = getRecord();
        ConsumerMessage msg = null;

        if (record != null) {
            partitionId = record.partition();
            key = String.valueOf(record.key()).getBytes();
            value = String.valueOf(record.value()).getBytes();
            offset = record.offset();
            timestamp = record.timestamp();
            timestampType = record.timestampType();
            checksum = record.checksum();
            serializedKeySize = record.serializedKeySize();
            serializedValueSize = record.serializedValueSize();

            msg = new ConsumerMessage(partitionId, offset, timestamp, timestampType,
                    checksum, serializedKeySize, serializedValueSize, key, value);
        }

        return msg;
    }



    public Collection<ConsumerMessage> receive(int size) {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        long timestamp;
        TimestampType timestampType;
        long checksum;
        int serializedKeySize;
        int serializedValueSize;
        int size_i = 0;
        Collection<ConsumerMessage> msgList = new ArrayList<ConsumerMessage>();
        //int partitionNum = topicPartitions.size();

        //System.out.println("Topic Partitions Number: " + String.valueOf(partitionNum));
        //System.out.println("Record List Size: " + String.valueOf(recordsListSize));
        System.out.println("Records Size: " + String.valueOf(recordsSize));

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
            timestamp = record.timestamp();
            timestampType = record.timestampType();
            checksum = record.checksum();
            serializedKeySize = record.serializedKeySize();
            serializedValueSize = record.serializedValueSize();

            ConsumerMessage msg = new ConsumerMessage(partitionId, offset, timestamp, timestampType,
                    checksum, serializedKeySize, serializedValueSize, key, value);
            msgList.add(msg);
            size_i ++;
        }

        return msgList;
    }

    public void seek(int partitionId, long offset) {

        TopicPartition topicPartition = new TopicPartition(topic, partitionId);

        consumer.poll(1);

        consumer.seek(topicPartition, 0);

    }

    public void close () {
        consumer.close();
    }
}
