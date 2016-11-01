/**
 * Created by cuixuan on 9/28/16.
 */
package kafkaimp08;

import api.ConsumerMessage;
import api.MessageConsumer;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Consumer;
import kafka.message.MessageAndMetadata;
//import kafka.javaapi.consumer.ZookeeperConsumerConnector;
import kafka.common.TopicAndPartition;
//import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.hadoop.conf.Configuration;
//import org.apache.kafka.common.TopicPartition;
//import org.apache.kafka.common.record.TimestampType;

import java.util.*;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

//import scala.Int;
//import scala.collection.JavaConversions;

public class KafkaMessageConsumer implements MessageConsumer {

    private String topic;
//    private ZookeeperConsumerConnector consumerConnector = null;
    private ConsumerConnector consumerConnector = null;
    ConsumerIterator<byte[], byte[]> it = null;
    private int messageNum = 0;
    //private int messageIndex = -1;
    private int streamsNum = 0;
    List<KafkaStream<byte[], byte[]>> streams;
    List<MessageAndMetadata> messageAndMetadatas = new ArrayList<>();
    private ExecutorService executorService;
    private int threadsNum = 1;
    private KafkaStream<byte[], byte[]> curStream;

    private MessageAndMetadata getRecord() {

//        if (messageIndex >= messageNum) {
//            System.out.println("no available message.");
//            return null;
//        }
        if (!it.hasNext()) {
            System.out.println("no available message.");
            return null;
        }
        // there is no available records memory
//        if (messageIndex == -1) {
//
//            int threadNumber = 0;
//            for (final KafkaStream<byte[], byte[]> stream : streams) {
//                //this.executorService.submit(new ConsumerThread(stream, threadNumber));
//                Thread consumerThread = new ConsumerThread(stream, threadNumber);
//                consumerThread.start();
//                try {
//                    consumerThread.join();
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//
//                threadNumber++;
//            }

//            try {
//                Thread.sleep(10000);
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//            executorService.shutdown();
//            messageIndex ++;
//        }
        MessageAndMetadata messageAndMetadata = it.next();
//        MessageAndMetadata messageAndMetadata = messageAndMetadatas.get(messageIndex);
//        messageIndex ++;
        //TopicAndPartition topicAndPartition = new TopicAndPartition(topic, (int)messageAndMetadata.offset());
        //consumerConnector.commitOffsetToZooKeeper(topicAndPartition, streamIndex);
        return messageAndMetadata;
    }

    public KafkaMessageConsumer(Configuration conf, String topicOut) {

        topic = topicOut;
        Properties props = new Properties();
        Map<String, String> defaults = new HashMap<>();
        Map<String, String> overrides = new HashMap<>();
        ConsumerConfig consumerConfig = null;

        defaults.put("zookeeper.connect", "localhost:2181");
        defaults.put("auto.offset.reset", "smallest");
        defaults.put("group.id", "group1");
        defaults.put("zookeeper.session.timeout.ms", "400");
        defaults.put("zookeeper.sync.time.ms", "200");
        defaults.put("auto.commit.intervals.ms", "1000");
        defaults.put("bootstrap.servers", "localhost:9092");
        defaults.put("enable.auto.commit", "true");
        defaults.put("session.timeout.ms", "30000");
        defaults.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        defaults.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //defaults.put("consumer.timeout.ms", "1000");

        for (Iterator<Map.Entry<String, String>> conf_i = conf.iterator(); conf_i.hasNext(); ) {
            Map.Entry<String, String> entry = conf_i.next();
            overrides.put(entry.getKey(), entry.getValue());
        }

        props.putAll(defaults);
        props.putAll(overrides);

        consumerConfig = new ConsumerConfig(props);
//        consumerConnector = new ZookeeperConsumerConnector(consumerConfig);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topicOut, new Integer(threadsNum));
//        Map<String, Object> topicCountMap = new HashMap<>();
//        topicCountMap.put(topicOut, Int.unbox(new Integer(1)));
//        StringDecoder keydecoder = new StringDecoder(new VerifiableProperties());
//        StringDecoder valuedecoder = new StringDecoder(new VerifiableProperties());
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap =
                consumerConnector.createMessageStreams(topicCountMap); // just call one time
        streams = consumerMap.get(topic);
        streamsNum = streams.size();
        System.out.println("streamsNum: " + String.valueOf(streamsNum));

        executorService = Executors.newFixedThreadPool(threadsNum);
        curStream = streams.get(0);
        it = curStream.iterator();

    }

    private class ConsumerThread extends Thread{
        private final int threadSerial;
        private final KafkaStream<byte[], byte[]> stream;

        ConsumerThread(KafkaStream<byte[], byte[]> streamOut, int threadSerialOut) {
            threadSerial = threadSerialOut;
            stream = streamOut;
        }

        public void run() {
            ConsumerIterator<byte[], byte[]> iter = this.stream.iterator();
            String fileName = threadSerial + ".data";
            System.out.println("Start Thread: " + this.threadSerial);
            int cnt = 0;
            try {
                while (iter.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> messageAndMetadata = iter.next();
                    messageAndMetadatas.add(messageAndMetadata);
                    cnt++;
                }
            } catch (kafka.consumer.ConsumerTimeoutException e) {
                messageNum = messageAndMetadatas.size();
                System.out.println("messageNum: " + String.valueOf(messageNum));
                System.out.println("Shutting down Thread: " + this.threadSerial);
                //System.exit(0);
                return;
            }

        }
    }

    public ConsumerMessage receive () {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        long timestamp;
        ConsumerMessage msg = null;

        MessageAndMetadata<byte[], byte[]> record = getRecord();
        if (record != null) {
            partitionId = record.partition();
            key = record.key();
            value = record.message();
            offset = record.offset();
            //timestamp = record.timestamp();
            msg = new ConsumerMessage(partitionId, offset, key, value);
        }
        else {
            System.out.println("no available message.");
        }
        return msg;
    }



    public Collection<ConsumerMessage> receive(int size) {

        int partitionId = 0;
        byte[] key = null;
        byte[] value = null;
        long offset = 0;
        //long timestamp;
        int size_i = 0;
        Collection<ConsumerMessage> msgList = new ArrayList<ConsumerMessage>();

        MessageAndMetadata<byte[], byte[]> record = null;
        while (size_i < size) {
            record = getRecord();
            if (record == null) {
                return msgList;
            }
            partitionId = record.partition();
            key = record.key();
            value = record.message();
            offset = record.offset();
            //timestamp = record.timestamp();

            ConsumerMessage msg = new ConsumerMessage(partitionId, offset, key, value);
            msgList.add(msg);
            size_i ++;
        }

        return msgList;
    }

    public void seek(int partitionId, long offset) {

//        TopicPartition topicPartition = new TopicPartition(topic, partitionId);
//
//        consumerConnector.poll(1);
//
//        consumerConnector.seek(topicPartition, 0);

    }

    public void close () {
        consumerConnector.shutdown();
    }
}
