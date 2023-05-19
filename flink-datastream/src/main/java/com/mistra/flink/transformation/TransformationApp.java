package com.mistra.flink.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wrmistra@gmail.com
 * @date 2023/5/19
 * @ Description:
 */
public class TransformationApp {

    public static void main(String[] args) throws Exception {
        // 获取执行上下文环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        map(env);
        env.execute("TransformationApp");
    }

    /**
     * DataStream->DataStream
     * 将map算子对应的函数作用到DataStream，产生一个新的DataStream
     * map会作用到已有的DataStream这个数据集中的每一个元素上
     */
    public static void map(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/env.log");
        SingleOutputStreamOperator<AccessLog> streamOperator = source.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new AccessLog(Integer.parseInt(split[0].trim()), split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        source.print();
        streamOperator.print();
    }

    /**
     * DataStream->DataStream
     * 过滤
     */
    public static void filter(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/env.log");
        SingleOutputStreamOperator<AccessLog> streamOperator = source.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new AccessLog(Integer.parseInt(split[0].trim()), split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        streamOperator.filter(new FilterFunction<AccessLog>() {
            @Override
            public boolean filter(AccessLog accessLog) throws Exception {
                return accessLog.getDuration() > 1000;
            }
        });
        streamOperator.print();
    }

    /**
     * DataStream->DataStream
     * 一个数据转多个
     */
    public static void flatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/env.log");
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String value : split) {
                    collector.collect(value);
                }
            }
        }).print();
    }

    /**
     * DataStream->KeyedStream
     * 相同key的记录会分到相同的分区，底层默认按照hash分区
     */
    public static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/env.log");
        SingleOutputStreamOperator<AccessLog> streamOperator = source.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new AccessLog(Integer.parseInt(split[0].trim()), split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        // 按domain分组并求和
        streamOperator.keyBy(new KeySelector<AccessLog, String>() {
            @Override
            public String getKey(AccessLog accessLog) throws Exception {
                return accessLog.getDomain();
            }
        }).sum("duration").print();

    }

    /**
     * KeyedStream->DataStream
     * 做求和之类的操作
     */
    public static void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.readTextFile("data/env.log");
        SingleOutputStreamOperator<AccessLog> streamOperator = source.map(new MapFunction<String, AccessLog>() {
            @Override
            public AccessLog map(String s) throws Exception {
                String[] split = s.split(",");
                return new AccessLog(Integer.parseInt(split[0].trim()), split[1].trim(), Integer.parseInt(split[2].trim()));
            }
        });
        // 按domain分组并求和
        streamOperator.keyBy(new KeySelector<AccessLog, String>() {
            @Override
            public String getKey(AccessLog accessLog) throws Exception {
                return accessLog.getDomain();
            }
        }).reduce(new ReduceFunction<AccessLog>() {
            @Override
            public AccessLog reduce(AccessLog accessLog, AccessLog t1) throws Exception {
                return null;
            }
        }).print();
    }
}
