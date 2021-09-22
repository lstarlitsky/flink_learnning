package com.mit.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * 批处理 wordcount
 *
 * @author mit
 * @date 2021/9/22 下午4:55
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        // 获取执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 数据集地址
        String inputPath = "/home/mit/my_project/big_data/flink/flink_learnning/src/main/resources/hello.txt";
        DataSet<String> inputDataSet = env.readTextFile(inputPath);

        DataSet<Tuple2<String, Integer>> resultDataSet = inputDataSet.flatMap(new MyFlatMap())
                .groupBy(0)
                .sum(1);

        resultDataSet.print();
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {
        /**
         * The core method of the FlatMapFunction. Takes an element from the input data set and transforms
         * it into zero, one, or more elements.
         *
         * @param value The input value.
         * @param out   The collector for returning result values.
         * @throws Exception This method may throw exceptions. Throwing an exception will cause the operation
         *                   to fail and may trigger recovery.
         */
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
