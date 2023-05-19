package com.mistra.flink.source;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author wrmistra@gmail.com
 * @date 2023/5/19
 * @ Description:
 */
public class SourceApp {

    public static void main(String[] args) throws Exception {
        // 获取执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 绑定数据源，可以来自于文件，socket端口，kafka，集合等等
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        // 获取并行度  1
        System.out.println("source并行度:" + source.getParallelism());
        // 接收到的数据进行过滤，
        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return !"pk".equalsIgnoreCase(s);
            }
        }).setParallelism(5);//设置并行度
        // 如果没有设置的话则为当前可用核心数
        System.out.println("filter并行度:" + filter.getParallelism());
        env.execute("SourceApp");

        // 添加kafka为数据源
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093");
        properties.setProperty("group.id", "test");
        DataStreamSource<String> dataStreamSource = env.addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties) {
        });
        System.out.println("kafka并行度:" + dataStreamSource.getParallelism());
        dataStreamSource.print();
    }
}
