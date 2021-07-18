package ccs.kafka.perform;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.PerformSnapshot;
import ccs.perform.util.SequencialPerformCounter;

public class PollingConsumerMain {
    /** ロガー */
    private static final Logger log = LoggerFactory.getLogger(PollingConsumerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String groupid = System.getProperty("ccs.perform.groupid", "defaultgroup");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        Properties properties = new Properties();
        // XXX `docker-compose ps`で取得したKafkaのExposeポートを設定する。
        String brokerList = "127.0.0.1:50902,127.0.0.1:60633,127.0.0.1:58246";
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupid);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        KafkaConsumer<String, Integer> consumer =
                new KafkaConsumer<>(properties);

        consumer.subscribe(List.of(topic));

        try {
            // トピックを指定してメッセージを送信する

            SequencialPerformCounter pc = new SequencialPerformCounter();
            for( int i=0 ; i != iter ; i++ ) {
                long st = System.nanoTime();
                long et = 0;

                while( (et = System.nanoTime()) - st < loop_ns) {
                    final ConsumerRecords<String, Integer> consumerRecords =
                            consumer.poll(Duration.ofMillis(1000));

                    consumerRecords.forEach( (it)->{
                        pc.perform(toInt( it.value(), -1));
                    });

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
