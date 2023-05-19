package com.mistra.flink.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
}
