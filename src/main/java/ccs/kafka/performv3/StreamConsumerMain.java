package ccs.kafka.performv3;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.WrapperSerde;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.CommonProperties;
import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;

public class StreamConsumerMain {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(StreamConsumerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        Properties properties = new Properties();
        // XXX `docker-compose ps`で取得したKafkaのExposeポートを設定する。
        String brokerList = CommonProperties.get("kafka.broker-list");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic_stream");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, new WrapperSerde<>(new LatencyMeasurePingSerializer(), new LatencyMeasurePingDeserializer()).getClass());

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, Integer> kStream = streamsBuilder.stream(topic);
        SequencialPerformCounter pc = new SequencialPerformCounter();

        kStream.foreach( (k, v)->{
            pc.perform(v);
        });

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), properties);
        streams.start();
        try {


            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;

                TimeUnit.NANOSECONDS.sleep(loop_ns);
                et = System.nanoTime();

                PerformSnapshot snap = pc.reset();
                snap.print(log, et-st);
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            streams.close();
        }
    }

    private static Integer toInt(Integer value, int i) {
        return value;
    }

    private static Integer toInt(String value, int i) {
        try {
            return Integer.parseInt(value);
        }catch (Exception e) {
            log.warn("parseError:{}",value);
            return i;
        }
    }
}
