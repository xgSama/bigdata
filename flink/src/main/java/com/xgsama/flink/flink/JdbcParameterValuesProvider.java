package com.xgsama.flink.flink;

import java.io.Serializable;

/**
 * JdbcParameterValuesProvider
 *
 * @author xgSama
 * @date 2021/4/19 15:55
 */
public interface JdbcParameterValuesProvider {

    Serializable[][] getParameterValues();
}
