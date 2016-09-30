/**
 * Created by cuixuan on 9/28/16.
 */
import org.apache.hadoop.conf.Configuration;
import java.util.List;

public class TestMain {

    public static void main(String args[]) {

        Configuration conf = new Configuration();
        //conf.set("zookeeper.connect", "localhost:2181");
        conf.set("group.id", "group1");
        conf.set("enable.auto.commit", "true");
        conf.set("session.timeout.ms", "1000");
        conf.set("acks", "all");
        conf.set("retries", "0");
        conf.set("sbatch.size", "16384");
        conf.set("linger.ms", "1");
        conf.set("buffer.memory", "335544");
        conf.set("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        conf.set("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        MessagingSystem messagingSystem = MessagingSystem.get(conf);

        System.out.println(String.valueOf(messagingSystem.createTopic("test2", 1, 0)));
        System.out.println(String.valueOf(messagingSystem.topicExists("test2")));

        MessageProducer messageProducer = messagingSystem.createProducer("test2");
        MessageConsumer messageConsumer = messagingSystem.createConsumer("test2");

        int count = 0;
        Message msg = new Message(0, null, null);
        while((count ++) != 10) {
            msg.value = ("msg-" + String.valueOf(count)).getBytes();
            messageProducer.sendAsync(msg);
        }
        messageProducer.close();
        System.out.println("Producer has send messages.");

        Message msgRec;
        msgRec = messageConsumer.receive();
        System.out.println("Single message:\nval: " + String.valueOf(msgRec.value) +
        " key: " + String.valueOf(msgRec.key) + " partitionId: " + String.valueOf(msgRec.partitionId) + "\n");

        int recCount = 4;
        System.out.println(String.valueOf(recCount) + " message:\n");
        List<Message> msgList = (List<Message>)messageConsumer.receive(recCount);
        int recCount_i = 0;
        while (recCount_i < recCount) {
            msgRec = msgList.get(recCount_i);
            System.out.println("val: " + String.valueOf(msgRec.value) +
                    " key: " + String.valueOf(msgRec.key) + " partitionId: " +
                    String.valueOf(msgRec.partitionId) + "\n");
            recCount_i ++;
        }

        messageProducer.close();
        messageConsumer.close();
        messagingSystem.close();

        System.out.println("\nAll done.\n");
    }
}
