package com.mistra.flink;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author wrmistra@gmail.com
 * @date 2023/5/11
 * @ Description: word count 样例
 */
public class StreamingApp {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 对接数据源
        DataStreamSource<String> source = env.socketTextStream("localshot", 9888);

        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> collector) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    collector.collect(s.toLowerCase().trim());
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return StringUtils.isNotEmpty(s);

            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        }).keyBy(0).sum(1).print();
    }
}
