package api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import java.lang.reflect.Constructor;

/**
 * Created by cuixuan on 10/11/16.
 */
public abstract class MessagingSystem {

    public static MessagingSystem get(Configuration conf) {

        String className = conf.get("hadoop.messaging.system");
        Class clazz = null;
        Constructor constructor = null;
        MessagingSystem messagingSystem = null;

        if (className == null) {
            return null;
        }
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            System.out.println("Class not found.");
        }

        try {
            constructor = clazz.getConstructor(Configuration.class);
        } catch (NoSuchMethodException e) {
            System.out.println("No such method.");
        }

        try {
            messagingSystem = (MessagingSystem) constructor.newInstance(conf);
        } catch (Exception e) {
            System.out.println("Invalid instance.");
        }
        
        return messagingSystem;
    }

    MessageProducer createProducer(String topic) {
        return this.createProducer(topic);
    }

    MessageConsumer createConsumer(String topic) {
        return this.createConsumer(topic);
    }

    boolean topicExists(String topic) {
        return this.topicExists(topic);
    }

    boolean createTopic(final String topic, final int partitions, final int replica) {
        return this.createTopic(topic, partitions, replica);
    }

    void close() {

    }
}
