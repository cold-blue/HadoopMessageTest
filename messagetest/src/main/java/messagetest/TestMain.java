/**
 * Created by cuixuan on 9/28/16.
 */
package messagetest;
//import MessagingSystem;

import api.ConsumerMessage;
import api.ProducerMessage;
//import kafkaimp.KafkaMessageConsumer;
//import kafkaimp.KafkaMessageProducer;
//import kafkaimp.KafkaMessagingSystem;
//import kafkaimp.KafkaMessageConsumer;
//import kafkaimp.KafkaMessageProducer;
//import kafkaimp.KafkaMessagingSystem;
import kafkaimp08.KafkaMessageConsumer;
import kafkaimp08.KafkaMessageProducer;
import kafkaimp08.KafkaMessagingSystem;
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
        //conf.set("auto.offset.reset", "earliest");
        conf.set("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //conf.set("max.partition.fetch.bytes", "4096");

        //conf.set("hadoop.messaging.system", "kafkaimp.KafkaMessagingSystem");
        conf.set("hadoop.messaging.system", "kafkaimp08.KafkaMessagingSystem");
        conf.set("hadoop.messaging.kafka.bootstrap.servers", "localhost:9092");

        KafkaMessagingSystem messagingSystem = (KafkaMessagingSystem)KafkaMessagingSystem.get(conf);

        String topic = "test12";

        System.out.println(String.valueOf(messagingSystem.createTopic(topic, 4, 1)));

        System.out.println(String.valueOf(messagingSystem.topicExists(topic)));

        class MyThread extends Thread {
            public void run() {
                KafkaMessageProducer messageProducer = messagingSystem.createProducer(topic);

                int count = 0;
                ProducerMessage msg = new ProducerMessage(null, null);

                while((count ++) != 10) {
                    msg.setValue((String.valueOf(this.getId()) + "msg-" + String.valueOf(count)).getBytes());
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
                ConsumerMessage msgRec;

                msgRec = messageConsumer.receive();
                System.out.println("Single message:\noffset: " + String.valueOf(msgRec.offset()) + " val: " +
                        new String(msgRec.value()) + " key: " + new String(msgRec.key()) + " partitionId: " +
                        String.valueOf(msgRec.partitionId()) + "\n");

                int recCount = 5;
                System.out.println(String.valueOf(recCount) + " messages:\n");
                Collection<ConsumerMessage> msgCol = messageConsumer.receive(recCount);
                List<ConsumerMessage> msgList = new ArrayList<>();
                msgList.addAll(msgCol);
                int recCount_i = 0;
                while (recCount_i < recCount) {
                    msgRec = msgList.get(recCount_i);
                    System.out.println("offset: " + String.valueOf(msgRec.offset()) + " val: " +
                            new String(msgRec.value()) + " key: " + new String(msgRec.key()) + " partitionId: " +
                            String.valueOf(msgRec.partitionId()) + "\n");
                    System.out.println(msgRec.toString());
                    recCount_i ++;
                }

                messageConsumer.close();

            }
        }

        MyThread thread1 = new MyThread();
        thread1.start();
        try {
            thread1.join();

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        MyThread2 thread1_2 = new MyThread2();
        //MyThread thread2 = new MyThread();
        //MyThread2 thread2_2 = new MyThread2();

        thread1_2.start();
//        try {
//            Thread.sleep(300);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        //thread2.start();
        //thread2_2.start();
        //thread3.start();

        try {
            //thread1.join();
            thread1_2.join();
            //thread2.join();
            //thread2_2.join();
            //thread3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        messagingSystem.close();

        System.out.println("\nAll done.\n");


//        kafkaImp.KafkaMessageProducer messageProducer = messagingSystem.createProducer(topic);
//        kafkaImp.KafkaMessageConsumer messageConsumer = messagingSystem.createConsumer(topic);
//
//        int count = 0;
//        ProducerMessage msg = new ProducerMessage(0, null, null);
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
//        ProducerMessage msgRec;
//        msgRec = messageConsumer.receive();
//        System.out.println("Single message:\nval: " + new String(msgRec.value) +
//        " key: " + new String(msgRec.key) + " partitionId: " + String.valueOf(msgRec.partitionId) + "\n");
//
//        int recCount = 5;
//        System.out.println(String.valueOf(recCount) + " messages:\n");
//        Collection<ProducerMessage> msgCol = messageConsumer.receive(recCount);
//        List<ProducerMessage> msgList = new ArrayList<>();
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
