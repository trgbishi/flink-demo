package org.myorg.flink;


import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;


/**
 * 该类仅仅是一个kafka的连接测试
 * 当指定了kafka端口及zookeeper端口 及 kafka topic
 * 便能监听kafka该topic的数据
 * 本demo将监听到的数据 过滤 string->double-> 大于5
 * 再打印到控制台
 */
public class KafkaDemo {

    public static void main(String[] args) throws Exception {

        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //默认情况下，检查点被禁用。要启用检查点，请在StreamExecutionEnvironment上调用enableCheckpointing(n)方法，
        // 其中n是以毫秒为单位的检查点间隔。每隔5000 ms进行启动一个检查点,则下一个检查点将在上一个检查点完成后5秒钟内启动
        env.enableCheckpointing(5000);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "10.0.99.102:9092");
        properties.setProperty("zookeeper.connect", "10.0.99.102:2181");

        //一个group.id只能给一个消费者用
        properties.setProperty("group.id", "flink-cousumer-group");
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>("test-kafka-flink", new SimpleStringSchema(),
                properties);
        myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());
        DataStream<String> keyedStream = env.addSource(myConsumer).filter((s)->{
            int a = Integer.parseInt(s);
            if(a>5) {
                return true;
            }
            else {
                return false;
            }
        });
//        keyedStream.
        keyedStream.print();
        // execute program
        env.execute("Flink Streaming Java API Skeleton");

    }
}


//nohup bin/kafka-server-start.sh config/server.properties > my_kafkaserver.log 2>&1 &
//bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test-kafka-flink
//bin/kafka-console-producer.sh --broker-list localhost:9092 --topic test-kafka-flink
