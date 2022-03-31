package com.xgsama.flink.func.udaf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * CountUDAF
 *
 * @author : xgSama
 * @date : 2021/10/18 10:10:41
 */
public class CountUDAF extends AggregateFunction<Long, CountUDAF.CountAccum> {

    //定义存放count UDAF状态的accumulator的数据的结构。
    public static class CountAccum {
        public long total;
    }


    @Override
    public CountAccum createAccumulator() {
        CountAccum acc = new CountAccum();
        acc.total = 0;
        return acc;
    }

    @Override
    public Long getValue(CountAccum accumulator) {
        return accumulator.total;
    }

    /**
     * accumulate提供了如何根据输入的数据更新count UDAF存放状态的accumulator。
     *
     * @param accumulator 累加器
     * @param iValue      函数输入
     */
    public void accumulate(CountAccum accumulator, Integer iValue) {
        accumulator.total += iValue;
    }

    public void merge(CountAccum accumulator, Iterable<CountAccum> its) {
        for (CountAccum other : its) {
            accumulator.total += other.total;
        }

    }


}
