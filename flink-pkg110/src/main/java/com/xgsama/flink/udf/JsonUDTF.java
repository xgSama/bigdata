package com.xgsama.flink.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.List;

import java.util.Iterator;

/**
 * JsonUDTF
 *
 * @author : xgSama
 * @date : 2021/12/2 14:13:20
 */
public class JsonUDTF extends TableFunction<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonUDTF.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING,
                Types.STRING, Types.STRING, Types.STRING);
    }

    /**
     * @param input 输入参数
     * @return 返回参数
     */
    public void eval(String input) {
        try {
            if (!StringUtils.isNullOrWhitespaceOnly(input)) {
                String N2Out = "";
                Row row = new Row(17);
                JsonNode jsonNode = objectMapper.readTree(input);
                if (!StringUtils.isNullOrWhitespaceOnly(jsonNode.get("title").asText())
                        && !StringUtils.isNullOrWhitespaceOnly(jsonNode.get("updateTime").asText())) {

                    row.setField(0, jsonNode.get("itemId").asText());
                    row.setField(1, jsonNode.get("isDup").asText());
                    row.setField(2, jsonNode.get("copyright").asText());
                    row.setField(3, jsonNode.get("title").asText());
                    row.setField(4, jsonNode.get("publishTime").asText());
                    row.setField(5, jsonNode.get("source").asText());
                    row.setField(6, jsonNode.get("operationType").asText());

                    List<JsonNode> category = jsonNode.get("autoTags").findValues("category");
                    List<JsonNode> tagCode = jsonNode.get("autoTags").findValues("tagCode");
                    List<JsonNode> tagName = jsonNode.get("autoTags").findValues("tagName");
                    List<JsonNode> source = jsonNode.get("autoTags").findValues("source");
                    List<JsonNode> level = jsonNode.get("autoTags").findValues("level");
                    List<JsonNode> weight = jsonNode.get("autoTags").findValues("weight");
                    List<JsonNode> emotion = jsonNode.get("autoTags").findValues("emotion");
                    List<JsonNode> emotionWeight = jsonNode.get("autoTags").findValues("emotionWeight");

                    JsonNode autoTags = jsonNode.get("autoTags");
                    List<JsonNode> tagSystems1 = autoTags.findValues("tagSystems");

                    List<JsonNode> tagSystems = jsonNode.get("autoTags").findValues("tagSystems");

                    int i = 0;
                    for (JsonNode node : tagSystems) {
                        row.setField(7, category.get(i).asText());
                        row.setField(8, tagCode.get(i).asText());
                        row.setField(9, tagName.get(i).asText());
                        row.setField(10, source.get(i).asText());
                        row.setField(11, level.get(i).asText());
                        row.setField(12, weight.get(i).asText());
                        row.setField(13, emotion.get(i).asText());
                        row.setField(14, emotionWeight.get(i).asText());
                        i++;
                        Iterator<JsonNode> N2Node = node.elements();
                        //如何拿到小的數組中的數組個數放在for循環中
                        while (N2Node.hasNext()) {
                            N2Out += N2Node.next().asText() + "#";
                        }
                        if (!StringUtils.isNullOrWhitespaceOnly(N2Out) && N2Out.length() > 0) {
                            N2Out = N2Out.substring(0, N2Out.length() - 1);
                            row.setField(15, N2Out);
                            row.setField(16, jsonNode.get("updateTime").asText());
                        } else {

                        }

                        collect(row);

                        N2Out = "";
                    }
                }
            }
        } catch (Exception e) {
            LOG.error(e.toString());
        }
    }
}
