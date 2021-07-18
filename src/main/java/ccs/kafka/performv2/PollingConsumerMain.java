package ccs.kafka.performv2;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.CommonProperties;
import ccs.perform.util.SequencialPerformCounter;
import ccs.perform.util.PerformHistogram;
import ccs.perform.util.PerformSnapshot;

public class PollingConsumerMain {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(PollingConsumerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test3");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        Properties properties = new Properties();
        // XXX `docker-compose ps`で取得したKafkaのExposeポートを設定する。
        String brokerList = CommonProperties.get("kafka.broker-list");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LatencyMeasurePingDeserializer.class);
        KafkaConsumer<String, LatencyMeasurePing> consumer =
                new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        PerformHistogram hist = new PerformHistogram();
        hist.addShutdownHook();

        try {
            // トピックを指定してメッセージを送信する

            SequencialPerformCounter pc = new SequencialPerformCounter();
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;

                long totalLatency = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    final ConsumerRecords<String, LatencyMeasurePing> consumerRecords =
                            consumer.poll(Duration.ofMillis(1000));

                    for(ConsumerRecord<String, LatencyMeasurePing> it : consumerRecords){
                        pc.perform(getSeq( it.value(), -1));
                        long latency = it.value().getLatency();
                        pc.addLatency(latency);
                        hist.increament(latency);
                    };

                    consumer.commitAsync();
                }

                PerformSnapshot snap = pc.reset();
                snap.print(log, et-st);
            }
        } catch( Throwable th ) {
            th.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    private static Integer getSeq(LatencyMeasurePing value, int i) {
        return value.getSeq();
    }

    private static Integer getSeq(Integer value, int i) {
        return value;
    }

    private static Integer getSeq(String value, int i) {
        try {
            return Integer.parseInt(value);
        }catch (Exception e) {
            log.warn("parseError:{}",value);
            return i;
        }
    }
}
