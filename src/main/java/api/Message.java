package api;

/**
 * Created by cuixuan on 9/28/16.
 */

public class Message {

    public int partitionId = -1;
    public byte[] key;
    public byte[] value;

    public Message(int partitionId, byte[] key, byte[] value) {

        this.partitionId = partitionId;
        this.key = key;
        this.value = value;
    }

    public Message(byte[] key, byte[] value) {

        this.key = key;
        this.value = value;
    }
}
