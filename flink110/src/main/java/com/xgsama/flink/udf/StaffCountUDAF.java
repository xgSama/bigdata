package com.xgsama.flink.udf;

import com.alibaba.fastjson.JSONObject;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.functions.AggregateFunction;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * StaffCountUDAF
 *
 * @author : xgSama
 * @date : 2022/1/5 13:50:42
 */
@Slf4j
public class StaffCountUDAF extends AggregateFunction<String, Cache<String, Object>> {
    /**
     * 定义如何根据输入更新Accumulator
     */
    public void accumulate(Cache<String, Object> acc, String ptfloSno, String appDate, String ordAmt, String ptfloOpType,
                           String iaInvestAgrNo, String cancelFlag, String ptfloOrdStat,
                           String opertion, String comrate, String staffno, String curScale, String timeS) {
        //存储对象的主键
        String keyNo = ptfloSno.concat(appDate);
        //存储根据员工编码+日期主键
        String staffNo = staffno.concat(appDate);

        acc.put("staffNo", staffNo);
        //判断传入值是否纳入统计 True 纳入统计  False 不纳入统计
        Boolean statFlag = !("1".equals(cancelFlag) || "DELETE".equals(opertion) || Arrays.asList(new String[]{"4", "7"}).contains(ptfloOrdStat));

        //根据传入值生成 staffAmt 对象
        StaffAmt staffAmt = new StaffAmt(ptfloSno, appDate, ordAmt, ptfloOpType, iaInvestAgrNo, cancelFlag, ptfloOrdStat,
                opertion, comrate, staffno, curScale, timeS, statFlag);

        Object obj = acc.getIfPresent(staffNo);
        Map<String, List> map = JSONObject.parseObject(JSONObject.toJSONString(obj), HashMap.class);

        // 新的主键 存储对象并且放到 存储 List 的 Map中
        if (acc.getIfPresent(keyNo) == null) {
            acc.put(keyNo, staffAmt);
            if (map == null) { // 如果 map 为空。 则new List 放到map中
                map = new HashMap<>();
                List<String> list = new ArrayList();
                list.add(keyNo);
                map.put(staffNo, list);
            } else {
                List<String> list = map.get(staffNo);
                list.add(keyNo);
            }
        } else {
            StaffAmt oStaffAmt = (StaffAmt) acc.getIfPresent(keyNo);
            //新加入的时间值大于已存的时间值（数据是最新的）
            if (oStaffAmt.getStatFlag() && staffAmt.getTimeS().compareTo(oStaffAmt.getTimeS()) > 0) {
                try {
                    acc.put(keyNo, staffAmt);
                } catch (Exception e) {
                    log.error("put错误", e);
                }

            }
        }
    }

    @Override
    public String getValue(Cache<String, Object> acc) {

        String staffNo = String.valueOf(acc.getIfPresent("staffNo"));
        return acc.toString() + UUID.randomUUID().toString();
    }

    @Override
    public Cache<String, Object> createAccumulator() {
        Cache<String, Object> cache = Caffeine.newBuilder()
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .maximumSize(10_000)
                .build();
        return cache;
    }

    public void merge(Cache<String, Object> accumulator, Iterable<Cache<String, Object>> its) {
        for (Cache<String, Object> it : its) {
            accumulator.putAll(it.asMap());
        }
    }
}
