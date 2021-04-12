package com.xgsama.flink.flinkx;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * BaseRichInputFormatBuilder
 *
 * @author xgSama
 * @date 2021/1/29 14:29
 */
public abstract class BaseRichInputFormatBuilder {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected BaseRichInputFormat format;

    public void setMonitorUrls(String monitorUrls) {
        format.monitorUrls = monitorUrls;
    }

    public void setBytes(long bytes) {
        format.bytes = bytes;
    }


    public void setDataTransferConfig(DataTransferConfig dataTransferConfig){
        format.setDataTransferConfig(dataTransferConfig);
    }
    /**
     * Check the value of parameters
     */
    protected abstract void checkFormat();

    public BaseRichInputFormat finish() {
        Preconditions.checkNotNull(format);
        return format;
    }

}
