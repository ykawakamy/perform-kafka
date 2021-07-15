package ccs.kafka.performv2;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.CommonProperties;

public class ProducerMain {
    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test3");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        Properties properties = new Properties();
        // XXX `docker-compose ps`で取得したKafkaのExposeポートを設定する。
        String brokerList = CommonProperties.get("kafka.broker-list");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // Producer を構築する
        KafkaProducer<String, LatencyMeasurePing> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new LatencyMeasurePingSerializer());

        try {
            // トピックを指定してメッセージを送信する
            int seq = 0;
            for( int i=0 ; i != iter ; i++ ) {
                int cnt =0;
                long st = System.nanoTime();
                long et = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    LatencyMeasurePing value = new LatencyMeasurePing(seq);
                    producer.send(new ProducerRecord<String, LatencyMeasurePing>(topic, value ));
                    seq++;
                    cnt++;
                }

                log.info("{}: {} ns. {} times. {} ns/op", key, et-st, cnt, (et-st)/(double)cnt);
            }


            for (int i = 0; i != iter; i++) {
            }
        } finally {
            producer.close();
        }
    }
}
