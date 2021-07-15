
package ccs.kafka.performv2;

import org.apache.kafka.common.serialization.Serializer;

public class LatencyMeasurePingSerializer implements Serializer<LatencyMeasurePing> {

    @Override
    public byte[] serialize(String topic, LatencyMeasurePing data) {
        if (data == null)
            return null;

        return new byte[] {
                //----
                (byte) (data.seq >>> 24),
                (byte) (data.seq >>> 16),
                (byte) (data.seq >>> 8),
                (byte) (data.seq),
                //----
                (byte) (data.sendTick >>> 56),
                (byte) (data.sendTick >>> 48),
                (byte) (data.sendTick >>> 40),
                (byte) (data.sendTick >>> 32),
                (byte) (data.sendTick >>> 24),
                (byte) (data.sendTick >>> 16),
                (byte) (data.sendTick >>> 8),
                (byte) (data.sendTick)
        };
    }

}
