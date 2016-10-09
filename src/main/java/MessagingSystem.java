/**
 * Created by cuixuan on 9/28/16.
 */
import kafka.admin.*;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.InterruptException;
import scala.collection.JavaConversions;
import org.apache.hadoop.conf.Configuration;

import java.util.Collection;
import java.util.HashSet;


public class MessagingSystem {

    private ZkClient zkClient = new ZkClient("localhost:2181");
    private ZkConnection zkConnection = new ZkConnection("localhost:2181");
    private ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    private static MessagingSystem messagingSystem = null;
    private Configuration conf;

    public MessagingSystem(Configuration conf) {
        this.conf = conf;

    }

    public static MessagingSystem get (Configuration conf) {

        if (messagingSystem != null) {
            messagingSystem.conf = conf;
            return messagingSystem;
        }

        messagingSystem = new MessagingSystem(conf);
        return messagingSystem;
    }

    KafkaMessageProducer createProducer(String topic) {

        return new KafkaMessageProducer(conf, topic);
    }

    KafkaMessageConsumer createConsumer(String topic) {

        return new KafkaMessageConsumer(conf, topic);
    }

    boolean topicExists (String topic) {

        Collection<String> topicsCol = JavaConversions.asJavaCollection(zkUtils.getAllTopics());
        HashSet<String> topics = new HashSet<String>();
        topics.addAll(topicsCol);
        if (topics.contains(topic)) {
            return true;
        }
        else {
            return false;
        }
    }

    boolean createTopic(final String topic, final int partitions, final int replica) {

        Thread myThread = new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    String[] options = new String[] {"--zookeeper", "localhost:2181", "--create", "--topic", topic,
                            "--replication-factor", String.valueOf(replica), "--partitions", String.valueOf(partitions)};

                    TopicCommand.main(options);
                } catch (InterruptException e) {
                    System.out.println("InterruptException.");
                }
            }
        });

        try {

            Collection<String> topicsCol = JavaConversions.asJavaCollection(zkUtils.getAllTopics());
            HashSet<String> topics = new HashSet<String>();
            topics.addAll(topicsCol);
            if (topics.contains(topic)) {
                System.out.println("Topic exists.");
                return false;
            }

            myThread.start();;
            myThread.join();
            System.out.println("hello");
        }
        catch (Exception e) {
            System.out.println("Error: something wrong occurred when creating a topic.");
            return false;
        }
        return true;
    }

    void close () {
        messagingSystem = null;
    }
}
