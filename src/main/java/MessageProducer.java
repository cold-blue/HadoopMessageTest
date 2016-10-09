/**
 * Created by cuixuan on 9/30/16.
 */
public interface MessageProducer {
    int partitionId = 0;
    byte[] key = null;
    byte[] value = null;
    public MessageProducer(int partitionId, byte[] key, byte[] value);
}
