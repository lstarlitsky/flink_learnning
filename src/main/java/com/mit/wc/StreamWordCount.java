package com.mit.wc;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 流处理 wordcount
 *
 * @author mit
 * @date 2021/9/23 上午12:53
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度, 并行度默认为CPU核数
        env.setParallelism(1);

        // 数据集地址
        String inputPath = "/home/mit/my_project/big_data/flink/flink_learnning/src/main/resources/hello.txt";
        DataStream<String> inputDataStream = env.readTextFile(inputPath);

        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream = inputDataStream.flatMap(new WordCount.MyFlatMap())
                .keyBy(value -> value.f0)
                .sum(1);

        resultStream.print();

        // 执行任务
        env.execute();
    }
}
