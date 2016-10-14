package api;

import java.util.Collection;

/**
 * Created by cuixuan on 9/30/16.
 */
public interface MessageConsumer {
    ConsumerMessage receive();
    Collection<ConsumerMessage> receive(int size);
    void close();
}
