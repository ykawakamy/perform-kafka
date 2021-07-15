package ccs.kafka.perform;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerMain {
    private static final Logger log = LoggerFactory.getLogger(ProducerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test");
        String key = System.getProperty("ccs.perform.key", "defaultkey");
        long loop_ns = 5_000_000_000L; // ns = 5s
        int iter = Integer.valueOf(System.getProperty("ccs.perform.iterate", "20"));

        Properties properties = new Properties();
        // XXX `docker-compose ps`で取得したKafkaのExposeポートを設定する。
        String brokerList = "127.0.0.1:50902,127.0.0.1:60633,127.0.0.1:58246";
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);

        // Producer を構築する
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties, new StringSerializer(),
                new IntegerSerializer());

        try {
            // トピックを指定してメッセージを送信する
            int seq = 0;
            for( int i=0 ; i != iter ; i++ ) {
                int cnt =0;
                long st = System.nanoTime();
                long et = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    producer.send(new ProducerRecord<String, Integer>(topic, seq));
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
