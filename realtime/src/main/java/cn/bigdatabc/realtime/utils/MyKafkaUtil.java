package cn.bigdatabc.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * @description：TODO
 * @author     ：bboy枫亭
 * @date       ：Created in 2022/5/23 19:53
 */
public class MyKafkaUtil {
    private static String brokers = "node1:9092,node2:9092,node3:9092";
    private static String DEFAULT_TOPIC = "DWD_DEFAULT_TOPIC";

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(brokers, topic, new SimpleStringSchema());
    }
}
