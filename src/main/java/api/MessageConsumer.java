package api;

import java.util.Collection;

/**
 * Created by cuixuan on 9/30/16.
 */
public interface MessageConsumer {
    Message receive();
    Collection<Message> receive(int size);
    void close();
}
