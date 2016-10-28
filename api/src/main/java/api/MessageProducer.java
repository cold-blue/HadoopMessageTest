package api;
/**
 * Created by cuixuan on 9/30/16.
 */
public interface MessageProducer {
    void sendAsync(ProducerMessage msg);
    void close();
}
