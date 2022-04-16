import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LatencyWithGateway {
    private static final Logger log = LoggerFactory.getLogger(LatencyWithGateway.class);

    public static void main(String[] args) throws Exception {
        // initiate variables.
        String BOOTSTRAP_SERVERS = "127.0.0.1:9092";
        String TOPIC = "end2end";
        int NUMBER_OF_MESSAGE = 30;
        int TIME_OUT = 3000;
        double total_time = 0.0;
        double[] latencies = new double[NUMBER_OF_MESSAGE];

        // initiate both producer and consumer properties.
        Properties producerProperties = new Properties();
        producerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        producerProperties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"0");
        producerProperties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,String.valueOf(Long.MAX_VALUE));

        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"end2");
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        consumerProperties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
        consumerProperties.setProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,"0");

        // initiate producer and consumer object.
        KafkaProducer<String,String> producer = new KafkaProducer<>(producerProperties);
        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(consumerProperties);
        consumer.subscribe(Collections.singleton(TOPIC));

        for (int i = 0; i < NUMBER_OF_MESSAGE; i++) {
           String msg = "Message -> " + i;
           long begin = System.nanoTime();

           producer.send(new ProducerRecord<>(TOPIC,msg)).get();

           Iterator<ConsumerRecord<String,String>> recordIter = consumer.poll(Duration.ofMillis(TIME_OUT)).iterator();

           long elapsed = System.nanoTime() - begin;

           // if it cannot get msg.
            if (!recordIter.hasNext()) {
                finalise(producer,consumer);
                throw new RuntimeException(String.format("poll() timed out before finding a result (timeout:[%d] ms)",TIME_OUT));
            }

            // if msg read doesn't match the specific msg.
            String read = recordIter.next().value();
            if(!read.equals(msg)){
                finalise(producer,consumer);
                throw new RuntimeException(String.format("The message read [%s] did not match the message [%s]",read,msg));
            }

            // if it receives more than 1 msg.
            if (recordIter.hasNext()) {
                finalise(producer,consumer);
                throw new RuntimeException("Only one result was expected during this test. We found > 1 msg");
            }

            // Report progress
            if (i % 10 == 0)
                log.info(String.format("(Overview) latency msg %d = %.4f ",i+1,elapsed/1000.0/100.0));
            if (i != 0)
                total_time += elapsed;
            latencies[i] = elapsed / 1000.0 / 1000.0;
        }


        // Log results
        for(int i = 0; i < NUMBER_OF_MESSAGE; i++) {
            log.info(String.format("Latency of msg %d = %.4f ms",i+1,latencies[i]));
        }
        double avgTime = total_time/(NUMBER_OF_MESSAGE-1)/1000.0/1000.0;
        log.info(String.format("Avg latency: %.4f ms (not include msg 1)",avgTime));
        Arrays.sort(latencies);

        double p50 = latencies[(int)(latencies.length * 0.5)];
        double p99 = latencies[(int)(latencies.length * 0.99)];
        double p999 = latencies[(int)(latencies.length * 0.999)];
        log.info(String.format("Percentiles: 50th = %.4f",p50));
        log.info(String.format("Percentiles: 99th = %.4f",p99));
        log.info(String.format("Percentiles: 99.9th = %.4f",p999));

        // Push data to push gateway prometheus.
        pushDataToPrometheus(avgTime,p50,p99,p999);

        // Commit offset and close connection.
        finalise(producer,consumer);

        }

    public static void finalise(KafkaProducer<String,String> producer, KafkaConsumer<String,String> consumer){
        consumer.commitSync();
        consumer.close();
        producer.close();
    }

    public static void pushDataToPrometheus(double avgTime,double p50, double p99, double p999) throws IOException {
        CollectorRegistry registry = new CollectorRegistry();
        try {
            // push avg latency
            Gauge avgLatency = Gauge.build().
                    name("kafka_end_to_end_avg_latency_ms").
                    help("Average Kafka end-to-end latency(ms)").
                    register(registry);
            avgLatency.set(avgTime);
            // push percentile 50th
            Gauge p50Latency = Gauge.build().
                    name("kafka_end_to_end_p50_latency_ms").
                    help("Percentiles 50th Kafka end-to-end latency(ms)").
                    register(registry);
            p50Latency.set(p50);
            // push percentile 99th
            Gauge p99Latency = Gauge.build().
                    name("kafka_end_to_end_p99_latency_ms").
                    help("Percentiles 99th Kafka end-to-end latency(ms)").
                    register(registry);
            p99Latency.set(p99);
            // push percentile 99.9th
            Gauge p999Latency = Gauge.build().
                    name("kafka_end_to_end_p999_latency_ms").
                    help("Percentiles 99.9th Kafka end-to-end latency(ms)").
                    register(registry);
            p999Latency.set(p999);
        } finally {
            PushGateway pg = new PushGateway("127.0.0.1:9091");
            pg.pushAdd(registry, "kafka_end_to_end_latency");
        }
    }
}
