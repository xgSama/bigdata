package com.xgsama.flink.source;

import com.xgsama.flink.model.Student;
import com.xgsama.flink.util.SIterator;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.LongValueSequenceIterator;

import java.util.*;

/**
 * Demo
 *
 * @author : xgSama
 * @date : 2022/1/11 23:30:34
 */
public class Demo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Map<String, String> map = new HashMap<>();
        map.put("k1", "v1");

        HashSet<Integer> integers = new HashSet<>();
        SortedMap<Object, Object> objectObjectSortedMap = Collections.emptySortedMap();

        env.fromCollection(integers);
        env.execute();
    }
}
