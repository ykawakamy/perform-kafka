package ccs.kafka.performv3;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ccs.perform.util.CommonProperties;

public class RandomKeyProducerMain {
    private static final Logger log = LoggerFactory.getLogger(RandomKeyProducerMain.class);

    public static void main(String[] args) {
        String topic = System.getProperty("ccs.perform.topic", "test");
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

        Random rand = new Random();
        int range = 100;

        String[] seqkey = new String[range];
        for( int i=0 ; i<range ; i++ ) {
            seqkey[i] = key + "-" + i;
        }
        int[] seq = new int[range];

        try {
            // トピックを指定してメッセージを送信する
            for( int i=0 ; i != iter ; i++ ) {
                int sidx = rand.nextInt(range);
                int cnt =0;
                long st = System.nanoTime();
                long et = 0;
                while( (et = System.nanoTime()) - st < loop_ns) {
                    LatencyMeasurePing value = new LatencyMeasurePing(seq[sidx]);
                    Future<RecordMetadata> result = producer.send(new ProducerRecord<String, LatencyMeasurePing>(topic, seqkey[sidx],value ));
                   
                    seq[sidx]++;
                    cnt++;
                }

                log.info("{}: {} ns. {} times. {} ns/op", sidx, et-st, cnt, (et-st)/(double)cnt);
            }


        } finally {
            producer.close();
        }
    }
}
