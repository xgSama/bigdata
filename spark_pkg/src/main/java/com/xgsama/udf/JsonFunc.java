package com.xgsama.udf;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * JsonFunc
 *
 * @author : xgSama
 * @date : 2021/11/17 15:49:18
 */
public class JsonFunc extends UDF {

    public String evaluate(String json) {
        if (StringUtils.isEmpty(json)) {
            return null;
        }
        String key = "0_6_29.attribute_name_cn";
        if (StringUtils.isEmpty(key)) {
            return null;
        }
        try {
            String[] keyarr = key.split("\\.");
            JSONObject parse1 = (JSONObject) JSONObject.parse(json);
            JSONObject jsonObject = (JSONObject) parse1.get(keyarr[0]);
            String s = jsonObject.get(keyarr[1]).toString();

            return s;

        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }


    public static void main(String[] args) {

        String json = "{\"0_6_29\": {\"attribute_name_cn\": \"表行数\"}}";
        JsonFunc buildJson = new JsonFunc();
        System.out.println(buildJson.evaluate("{\"0_6_29\": {\"attribute_name_cn\": \"表行数\"}}"));
    }


}
