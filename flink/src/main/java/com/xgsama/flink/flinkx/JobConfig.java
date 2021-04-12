package com.xgsama.flink.flinkx;

import java.util.List;
import java.util.Map;

/**
 * JobConfig
 *
 * @author xgSama
 * @date 2021/1/29 14:32
 */
public class JobConfig {
    protected Map<String, Object> internalMap;
    public static final String KEY_SETTING_CONFIG = "setting";
    public static final String KEY_CONTENT_CONFIG_LIST = "content";

    private Map<String, String> setting;
    private List<Map<String, String>> content;


    public JobConfig(Map<String, Object> map) {

    }

}
