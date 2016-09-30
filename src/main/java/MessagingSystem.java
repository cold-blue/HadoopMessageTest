/**
 * Created by cuixuan on 9/28/16.
 */
import kafka.admin.*;
import kafka.utils.ZkUtils;
import kafka.utils.ZkUtils.*;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.zookeeper.ZKUtil;
import scala.collection.JavaConversions;
import org.apache.hadoop.conf.Configuration;

import java.util.Set;

public class MessagingSystem {

    private ZkClient zkClient = new ZkClient("localhost:2181");
    private ZkConnection zkConnection = new ZkConnection("localhost:2181");
    private ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    private static MessagingSystem messagingSystem = null;
    private Configuration conf;

    public MessagingSystem (Configuration conf) {
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

    MessageProducer createProducer(String topic) {

        return new MessageProducer(conf, topic);
    }

    MessageConsumer createConsumer(String topic) {

        return new MessageConsumer(conf, topic);
    }

    boolean topicExists (String topic) {

        Set<String> topics = (Set<String>) JavaConversions.asJavaCollection(zkUtils.getAllTopics());
        if (topics.contains(topic)) {
            return true;
        }
        else {
            return false;
        }
    }

    boolean createTopic(String topic, int partitions, int replica) {
        try {
            String[] options = new String[] {"--zookeeper", "localhost:2181", "--create", "--topic", topic,
                    "--replication-factor", String.valueOf(replica), "--partitions", String.valueOf(partitions)};

            TopicCommand.main(options);
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
