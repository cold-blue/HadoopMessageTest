package api;

/**
 * Created by cuixuan on 9/28/16.
 */

public class ProducerMessage {

    private Integer partitionId = null;
    private byte[] key;
    private byte[] value;
    private Long timestamp = null;

    public ProducerMessage(Integer partitionId, Long timestamp, byte[] key, byte[] value) {
        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp " + timestamp);

        this.partitionId = partitionId;
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
    }

    public ProducerMessage(int partitionId, byte[] key, byte[] value) {

        this(partitionId, null, key, value);
    }

    public ProducerMessage(byte[] key, byte[] value) {

        this(null, null, key, value);
    }

    public ProducerMessage(byte[] value) {

        this(null, null, null, value);
    }

    /**
     * @return The key (or null if no key is specified)
     */
    public byte[] key() {
        return key;
    }

    /**
     * @return The value
     */
    public byte[] value() {
        return value;
    }

    /**
     * @return The timestamp
     */
    public Long timestamp() {
        return timestamp;
    }

    /**
     * @return The partition to which the record will be sent (or null if no partition was specified)
     */
    public Integer partitionId() {
        return partitionId;
    }

    public void setKey(byte[] key) {

        this.key = key;
    }

    public void setValue(byte[] value) {

        this.value = value;
    }

    public void setTimestamp(Long timestamp) {

        if (timestamp != null && timestamp < 0)
            throw new IllegalArgumentException("Invalid timestamp " + timestamp);

        this.timestamp = timestamp;
    }

    public void setPartitionId(Integer partitionId) {

        this.partitionId = partitionId;
    }

    @Override
    public String toString() {
        String key = this.key == null ? "null" : this.key.toString();
        String value = this.value == null ? "null" : this.value.toString();
        String timestamp = this.timestamp == null ? "null" : this.timestamp.toString();
        return "ProducerMessage(partition=" + partitionId + ", key=" + key + ", value=" + value +
                ", timestamp=" + timestamp + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        else if (!(o instanceof ProducerMessage))
            return false;

        ProducerMessage that = (ProducerMessage) o;

        if (key != null ? !key.equals(that.key) : that.key != null)
            return false;
        else if (partitionId != null ? !partitionId.equals(that.partitionId) : that.partitionId != null)
            return false;
        else if (value != null ? !value.equals(that.value) : that.value != null)
            return false;
        else if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null)
            return false;

        return true;
    }
}
