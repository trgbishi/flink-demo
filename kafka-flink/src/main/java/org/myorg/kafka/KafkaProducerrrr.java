package org.myorg.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * kafka生产者
 */
public class KafkaProducerrrr implements Runnable {

    private final KafkaProducer<String, String> producer;
    private final String topic;
    public KafkaProducerrrr(String topicName) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.0.99.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(props);
        this.topic = topicName;
    }

    @Override
    public void run() {
        int messageNo = 1;
        try {
            for(;;) {
                String messageStr = String.valueOf(Math.random()*10);
                System.out.println(messageStr);
                producer.send(new ProducerRecord<>(topic, "Message", messageStr));

                //生产1000条就退出
                if(messageNo%10==0){
                    System.out.println("成功发送了"+messageNo+"条");
                    break;
                }
                messageNo++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    public static void main(String args[]) {
        KafkaProducerrrr test = new KafkaProducerrrr("test-kafka-flink");
        Thread thread = new Thread(test);
        thread.start();
    }
}