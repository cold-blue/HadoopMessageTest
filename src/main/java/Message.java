/**
 * Created by cuixuan on 9/28/16.
 */

public class Message {

    public int partitionId;
    public byte[] key;
    public byte[] value;

    public Message (int partitionId, byte[] key, byte[] value) {

        this.partitionId = partitionId;
        this.key = key;
        this.value = value;
    }
}
