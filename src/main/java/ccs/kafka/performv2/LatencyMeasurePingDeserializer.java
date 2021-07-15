
package ccs.kafka.performv2;

import org.apache.kafka.common.serialization.Deserializer;

public class LatencyMeasurePingDeserializer implements Deserializer<LatencyMeasurePing> {

    @Override
    public LatencyMeasurePing deserialize(String topic, byte[] data) {

        int seq = deserializeInt(data, 0);
        long tick = deserializeLong(data, 4);
        return new LatencyMeasurePing(seq, tick);
    }

    private int deserializeInt(byte[] data, int offset) {
        int value = 0;
        for (int i = 0; i < 4; i++) {
            value <<= 8;
            value |= data[i + offset] & 0xFF;
        }
        return value;
    }
    private long deserializeLong(byte[] data, int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value <<= 8;
            value |= data[i + offset] & 0xFF;
        }
        return value;
    }

}
