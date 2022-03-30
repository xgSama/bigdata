package com.xgsama.flink.udf;

import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.HashSet;
import java.util.Set;

/**
 * UDAFTest
 *
 * @author : xgSama
 * @date : 2021/12/7 15:49:42
 */
// 需求：⼀个输⼊流，包含学⽣期末成绩 根据输⼊数据流，取得每个学⽣总科⽬数的倍数
// {"name":"alix","age":16,"subject":"history","score":100,"gender":"female"}
public class UDAFTest extends AggregateFunction<Long, Tuple1<Long>> {


    public Set<String> subjectSet = new HashSet<>();

    @Override
    public Tuple1<Long> createAccumulator() {
        return new Tuple1<>(0L);
    }

    @Override
    public Long getValue(Tuple1<Long> accumulator) {
        return accumulator.f0 * 2;
    }

    /**
     * accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator。
     *
     * @param accumulator 累加器
     * @param iValue      函数输入
     */
    public void accumulate(Tuple1<Long> accumulator, String iValue) {
        subjectSet.add(iValue.trim());
        accumulator.f0 = ((long) subjectSet.size());
    }

    public void merge(Tuple1<Long> accumulator, Iterable<Tuple1<Long>> its) {
        for (Tuple1<Long> other : its) {
            accumulator.f0 += other.f0;
        }
    }
}
