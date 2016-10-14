package api;

import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.TimestampType;

/**
 * Created by cuixuan on 10/14/16.
 */
public class ConsumerMessage {
    public static final long NO_TIMESTAMP = Record.NO_TIMESTAMP;
    public static final int NULL_SIZE = -1;
    public static final int NULL_CHECKSUM = -1;

    private int partitionId;
    private byte[] key;
    private byte[] value;
    private long offset;
    private long timestamp;
    private TimestampType timestampType;
    private long checksum;
    private int serializedKeySize;
    private int serializedValueSize;

    public ConsumerMessage(int partitionId,
                           long offset,
                           long timestamp,
                           TimestampType timestampType,
                           long checksum,
                           int serializedKeySize,
                           int serializedValueSize,
                           byte[] key,
                           byte[] value) {
        this.partitionId = partitionId;
        this.offset = offset;
        this.timestamp = timestamp;
        this.timestampType = timestampType;
        this.checksum = checksum;
        this.serializedKeySize = serializedKeySize;
        this.serializedValueSize = serializedValueSize;
        this.key = key;
        this.value = value;
    }

    public ConsumerMessage(int partitionId, long offset, byte[] key, byte[] value) {

        this(partitionId, offset, NO_TIMESTAMP, TimestampType.NO_TIMESTAMP_TYPE,
                NULL_CHECKSUM, NULL_SIZE, NULL_SIZE, key, value);
    }

    public ConsumerMessage(byte[] key, byte[] value) {

        this.key = key;
        this.value = value;
    }

    /**
     * The partition from which this record is received
     */
    public int partitionId() {
        return this.partitionId;
    }

    /**
     * The key (or null if no key is specified)
     */
    public byte[] key() {
        return key;
    }

    /**
     * The value
     */
    public byte[] value() {
        return value;
    }

    /**
     * The position of this record in the corresponding Kafka partition.
     */
    public long offset() {
        return offset;
    }

    /**
     * The timestamp of this record
     */
    public long timestamp() {
        return timestamp;
    }

    /**
     * The timestamp type of this record
     */
    public TimestampType timestampType() {
        return timestampType;
    }

    /**
     * The checksum (CRC32) of the record.
     */
    public long checksum() {
        return this.checksum;
    }

    /**
     * The size of the serialized, uncompressed key in bytes. If key is null, the returned size
     * is -1.
     */
    public int serializedKeySize() {
        return this.serializedKeySize;
    }

    /**
     * The size of the serialized, uncompressed value in bytes. If value is null, the
     * returned size is -1.
     */
    public int serializedValueSize() {
        return this.serializedValueSize;
    }

    @Override
    public String toString() {
        return "ConsumerMessage(partitionId = " + partitionId() + ", offset = " + offset()
                + ", " + timestampType + " = " + timestamp + ", checksum = " + checksum
                + ", serialized key size = "  + serializedKeySize
                + ", serialized value size = " + serializedValueSize
                + ", key = " + key + ", value = " + value + ")";
    }
}
