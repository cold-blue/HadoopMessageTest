/**
 * Created by cuixuan on 9/28/16.
 */
//import api.MessagingSystem;
import api.Message;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TestMain {

    public static void main(String args[]) {

        Configuration conf = new Configuration();
        conf.set("zookeeper.connect", "localhost:2181");
        conf.set("bootstrap.servers", "localhost:9092");
        conf.set("acks", "all");
        conf.set("retries", "0");
        conf.set("batch.size", "16384");
        conf.set("linger.ms", "1");
        conf.set("buffer.memory", "335544");
        conf.set("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        conf.set("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        conf.set("group.id", "group1");
        conf.set("zookeeper.sync.time.ms", "200");
        conf.set("session.timeout.ms", "30000");
        conf.set("zookeeper.session.timeout.ms", "400");
        conf.set("auto.commit.intervals.ms", "1000");
        conf.set("auto.offset.reset", "earliest");
        conf.set("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        conf.set("hadoop.messaging.system", "KafkaMessagingSystem");
        conf.set("hadoop.messaging.kafka.bootstrap.servers", "localhost:9092");

        KafkaMessagingSystem messagingSystem = (KafkaMessagingSystem)KafkaMessagingSystem.get(conf);

        String topic = "test12";

        System.out.println(String.valueOf(messagingSystem.createTopic(topic, 4, 1)));

        System.out.println(String.valueOf(messagingSystem.topicExists(topic)));

        class MyThread extends Thread {
            public void run() {
                KafkaMessageProducer messageProducer = messagingSystem.createProducer(topic);

                int count = 0;
                Message msg = new Message(null, null);

                while((count ++) != 5) {
                    msg.value = (String.valueOf(this.getId()) + "msg-" + String.valueOf(count)).getBytes();
                    //msg.key = msg.value;
                    messageProducer.sendAsync(msg);
                }
                System.out.println("Producer has send messages.");

                messageProducer.close();

            }
        }

        class MyThread2 extends Thread {
            public void run() {
                KafkaMessageConsumer messageConsumer = messagingSystem.createConsumer(topic);
                Message msgRec;

                msgRec = messageConsumer.receive();
                System.out.println("Single message:\nval: " + new String(msgRec.value) +
                        " key: " + new String(msgRec.key) + " partitionId: " + String.valueOf(msgRec.partitionId) + "\n");

                int recCount = 2;
                System.out.println(String.valueOf(recCount) + " messages:\n");
                Collection<Message> msgCol = messageConsumer.receive(recCount);
                List<Message> msgList = new ArrayList<>();
                msgList.addAll(msgCol);
                int recCount_i = 0;
                while (recCount_i < recCount) {
                    msgRec = msgList.get(recCount_i);
                    System.out.println("val: " + new String(msgRec.value) +
                            " key: " + new String(msgRec.key) + " partitionId: " +
                            String.valueOf(msgRec.partitionId) + "\n");
                    recCount_i ++;
                }

                messageConsumer.close();

            }
        }

        MyThread thread1 = new MyThread();
        MyThread2 thread1_2 = new MyThread2();
        MyThread thread2 = new MyThread();
        MyThread2 thread2_2 = new MyThread2();
        thread1.start();
        thread1_2.start();
//        try {
//            Thread.sleep(300);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        thread2.start();
        thread2_2.start();
        //thread3.start();

        try {
            thread1.join();
            thread1_2.join();
            thread2.join();
            thread2_2.join();
            //thread3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        messagingSystem.close();

        System.out.println("\nAll done.\n");


//        KafkaMessageProducer messageProducer = messagingSystem.createProducer(topic);
//        KafkaMessageConsumer messageConsumer = messagingSystem.createConsumer(topic);
//
//        int count = 0;
//        api.Message msg = new api.Message(0, null, null);
//
//        while((count ++) != 5) {
//            msg.value = ("msg-" + String.valueOf(count)).getBytes();
//            messageProducer.sendAsync(msg);
//        }
//
//        System.out.println("Producer has send messages.");
//
//        count = 0;
//
//        while((count ++) != 5) {
//            msg.value = ("msg-" + String.valueOf(count)).getBytes();
//            messageProducer.sendAsync(msg);
//        }
//        System.out.println("Producer has send messages.");
//
//
//
//
//        api.Message msgRec;
//        msgRec = messageConsumer.receive();
//        System.out.println("Single message:\nval: " + new String(msgRec.value) +
//        " key: " + new String(msgRec.key) + " partitionId: " + String.valueOf(msgRec.partitionId) + "\n");
//
//        int recCount = 5;
//        System.out.println(String.valueOf(recCount) + " messages:\n");
//        Collection<api.Message> msgCol = messageConsumer.receive(recCount);
//        List<api.Message> msgList = new ArrayList<>();
//        msgList.addAll(msgCol);
//        int recCount_i = 0;
//        while (recCount_i < recCount) {
//            msgRec = msgList.get(recCount_i);
//            System.out.println("val: " + new String(msgRec.value) +
//                    " key: " + new String(msgRec.key) + " partitionId: " +
//                    String.valueOf(msgRec.partitionId) + "\n");
//            recCount_i ++;
//        }
//
//        messageProducer.close();
//        messageConsumer.close();
//        messagingSystem.close();
//
//        System.out.println("\nAll done.\n");
    }
}
