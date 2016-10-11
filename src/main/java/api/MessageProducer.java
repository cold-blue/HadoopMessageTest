package api;

/**
 * Created by cuixuan on 9/30/16.
 */
public interface MessageProducer {
    void sendAsync(Message msg);
    void close();
}
