package com.xgsama.flink.flinkx;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * BaseRichInputFormat
 *
 * @author xgSama
 * @date 2021/1/29 13:52
 */
public abstract class BaseRichInputFormat extends RichInputFormat<Row, InputSplit> {

    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected String jobName = "defaultJobName";
    protected String jobId;
    protected LongCounter numReadCounter;
    protected LongCounter bytesReadCounter;
    protected LongCounter durationCounter;
    protected String monitorUrls;
    protected long bytes;

    protected DataTransferConfig dataTransferConfig;

    /**
     * 有子类实现，打开数据连接
     *
     * @param inputSplit 分片
     * @throws IOException 连接异常
     */
    protected abstract void openInternal(InputSplit inputSplit) throws IOException;

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {


        return new InputSplit[0];
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return null;
    }

    @Override
    public void open(InputSplit split) throws IOException {

    }

    @Override
    public boolean reachedEnd() throws IOException {
        return false;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        return null;
    }

    @Override
    public void close() throws IOException {

    }


    public DataTransferConfig getDataTransferConfig() {
        return dataTransferConfig;
    }

    public void setDataTransferConfig(DataTransferConfig dataTransferConfig) {
        this.dataTransferConfig = dataTransferConfig;
    }
}
