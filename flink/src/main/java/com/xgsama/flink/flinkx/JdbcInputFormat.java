package com.xgsama.flink.flinkx;

import org.apache.flink.core.io.InputSplit;

import java.io.IOException;

/**
 * JdbcInputFormat
 *
 * @author xgSama
 * @date 2021/1/29 14:04
 */
public class JdbcInputFormat extends BaseRichInputFormat {
    @Override
    protected void openInternal(InputSplit inputSplit) throws IOException {

    }
}
