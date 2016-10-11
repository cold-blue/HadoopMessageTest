/**
 * Created by cuixuan on 9/28/16.
 */
import api.*;
import kafka.admin.AdminUtils;
import kafka.common.Topic;
import kafka.utils.ZkUtils;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;


import scala.collection.JavaConversions;

import java.util.Collection;
import java.util.HashSet;

public class KafkaMessagingSystem extends MessagingSystem implements Configurable {

    private int sessionTimeoutMs = 3 * 1000;
    private int connectionTimeoutMs = 2 * 1000;
    private ZkClient zkClient = new ZkClient("localhost:2181",
            sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    private ZkConnection zkConnection = new ZkConnection("localhost:2181");
    private ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    private static KafkaMessagingSystem messagingSystem = null;
    private Configuration conf;

    public KafkaMessagingSystem(Configuration conf) {
        this.conf = conf;

    }

    public void setConf(Configuration confOut) {
        conf = confOut;
    }

    public Configuration getConf() {
        return this.conf;
    }

//    public static KafkaMessagingSystem get (Configuration conf) {
//
//        if (messagingSystem != null) {
//            messagingSystem.conf = conf;
//            return messagingSystem;
//        }
//
//        messagingSystem = new KafkaMessagingSystem(conf);
//        return messagingSystem;
//    }

    public KafkaMessageProducer createProducer(String topic) {

        return new KafkaMessageProducer(conf, topic);
    }

    public KafkaMessageConsumer createConsumer(String topic) {

        return new KafkaMessageConsumer(conf, topic);
    }

    public boolean topicExists(String topic) {

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

    public boolean createTopic(final String topic, final int partitions, final int replica) {

        Collection<String> topicsCol = JavaConversions.asJavaCollection(zkUtils.getAllTopics());
        HashSet<String> topics = new HashSet<String>();
        topics.addAll(topicsCol);
        if (topics.contains(topic)) {
            System.out.println("Topic exists.");
            return false;
        }

        if (Topic.hasCollisionChars(topic)) {
            System.out.println("WARNING: Due to limitations in metric names, topics with a period ('.') or" +
                    " underscore ('_') could collide. To avoid issues it is best to use either, but not both.");
        }

        AdminUtils.createTopic(zkUtils, topic, partitions, replica,
                AdminUtils.createTopic$default$5(), AdminUtils.createTopic$default$6());
        System.out.println("Topic " + topic + " created.");

        return true;
    }

    public void close() {

        zkUtils.close();
        messagingSystem = null;
    }
}
