/**
 * Created by cuixuan on 9/28/16.
 */
import org.apache.kafka.clients.consumer.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class KafkaMessageConsumer {

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

        // there is no available records memory and it is needed to fetch more from the topic or topic list(to do)
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

        return record;
    }

    public KafkaMessageConsumer(Configuration conf, String topicOut) {

        topic = topicOut;
        Properties props = new Properties();

        String zookeeperConnect = conf.get("zookeeper.connect");
        String autoOffsetReset = conf.get("auto.offset.reset");
        String groupId = conf.get("group.id");
        String zookeeperSessionTimeoutMs = conf.get("zookeeper.session.timeout.ms");
        String zookeeperSyncTimeMs = conf.get("zookeeper.sync.time.ms");
        String autoCommitIntervalsMs = conf.get("auto.commit.intervals.ms");
        String bootstrapServers = conf.get("bootstrap.servers");
        //String enableAutoCommit = conf.get("enable.auto.commit");
        //String heartbeatInterval = conf.get("heartbeat.interval.ms");
        String sessionTimeoutMs = conf.get("session.timeout.ms");
        String keyDeserializer = conf.get("key.deserializer");
        String valueDeserializer = conf.get("value.deserializer");

        props.put("zookeeper.connect", zookeeperConnect); //"localhost:2181");
        props.put("auto.offset.reset", autoOffsetReset);//"earliest");
        props.put("group.id", groupId);//"group1");
        props.put("zookeeper.session.timeout.ms", zookeeperSessionTimeoutMs);// "400");
        props.put("zookeeper.sync.time.ms", zookeeperSyncTimeMs);// "200");
        props.put("auto.commit.intervals.ms", autoCommitIntervalsMs); // "1000");
        props.put("bootstrap.servers", bootstrapServers);// "localhost:9092");
        props.put("enable.auto.commit", "false");// "true"); //close auto commit
        //props.put("heartbeat.interval.ms", sessionTimeoutMs);// "1000");
        props.put("session.timeout.ms", sessionTimeoutMs);// "30000");
        props.put("key.deserializer", keyDeserializer);//"org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", valueDeserializer);// "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(topic));
    }

    KafkaMessage receive () {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        //long offset = 0;

        ConsumerRecord<String, String> record = getRecord();

        if (record != null) {
            partitionId = record.partition();
            key = String.valueOf(record.key()).getBytes();
            value = String.valueOf(record.value()).getBytes();
        }

//        offset = record.offset();
//
//        Map<TopicPartition, OffsetAndMetadata> offsets = Collections.singletonMap(topicPartition0, new OffsetAndMetadata(offset + 1));
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

        //System.out.println(String.valueOf(offset));
        KafkaMessage msg = new KafkaMessage(partitionId, key, value);
        return msg;
    }

    Collection<KafkaMessage> receive(int size) {

        long partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        //long offset = 0;
        int size_i = 0;
        Collection<KafkaMessage> msgList = new ArrayList<KafkaMessage>();

        int partitionNum = topicPartitions.size();
        System.out.println("Partition number: " + String.valueOf(partitionNum));

        ConsumerRecord<String, String> record = null;
        while (size_i < size) {
            record = getRecord();
            if (record == null) {
                return msgList;
            }
            partitionId = record.partition();
            key = String.valueOf(record.key()).getBytes();
            value = String.valueOf(record.value()).getBytes();
            //offset = record.offset();
            KafkaMessage msg = new KafkaMessage((int)partitionId, key, value);
            msgList.add(msg);
            size_i ++;
        }

        return msgList;
    }

    void close () {
        consumer.close();
    }
}
