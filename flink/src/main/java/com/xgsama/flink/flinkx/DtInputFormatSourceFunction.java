package com.xgsama.flink.flinkx;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * DtInputFormatSourceFunction
 *
 * @author xgSama
 * @date 2021/4/19 11:55
 */
public class DtInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(DtInputFormatSourceFunction.class);

    private TypeInformation<OUT> typeInfo;
    private transient TypeSerializer<OUT> serializer;

    private InputFormat<OUT, InputSplit> format;

    private transient InputSplitProvider provider;
    private transient Iterator<InputSplit> splitIterator;

    private volatile boolean isRunning = true;


    private static final String LOCATION_STATE_NAME = "data-sync-location-states";

    private boolean isStream;

    private Map<Integer, FormatState> formatStateMap;
    private transient ListState<FormatState> unionOffsetStates;


    @SuppressWarnings("unchecked")
    public DtInputFormatSourceFunction(InputFormat<OUT, ?> format, TypeInformation<OUT> typeInfo) {
        super(format, typeInfo);
        this.format = (InputFormat<OUT, InputSplit>) format;
        this.typeInfo = typeInfo;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        StreamingRuntimeContext context = (StreamingRuntimeContext) getRuntimeContext();

        if (format instanceof RichInputFormat) {
            ((RichInputFormat) format).setRuntimeContext(context);
        }

//        if (format instanceof BaseRichInputFormat) {
//            RestoreConfig restoreConfig = ((BaseRichInputFormat) format).getRestoreConfig();
//            isStream = restoreConfig != null && restoreConfig.isStream();
//            if (formatStateMap != null) {
//                ((BaseRichInputFormat) format).setRestoreState(formatStateMap.get(context.getIndexOfThisSubtask()));
//            }
//        }

        format.configure(parameters);

        provider = context.getInputSplitProvider();
        serializer = typeInfo.createSerializer(getRuntimeContext().getExecutionConfig());
//        splitIterator = getInputSplits();
        isRunning = splitIterator.hasNext();
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        FormatState formatState = ((BaseRichInputFormat) format).getFormatState();
        if (formatState != null) {
            LOG.info("InputFormat format state:{}", formatState.toString());
            unionOffsetStates.clear();
            unionOffsetStates.add(formatState);
        }
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        LOG.info("Start initialize input format state");

        OperatorStateStore stateStore = context.getOperatorStateStore();
        unionOffsetStates = stateStore.getUnionListState(new ListStateDescriptor<>(
                LOCATION_STATE_NAME,
                TypeInformation.of(new TypeHint<FormatState>() {
                })));

        LOG.info("Is restored:{}", context.isRestored());
        if (context.isRestored()) {
            formatStateMap = new HashMap<>(16);
            for (FormatState formatState : unionOffsetStates.get()) {
                formatStateMap.put(formatState.getNumOfSubTask(), formatState);
                LOG.info("Input format state into:{}", formatState.toString());
            }
        }

        LOG.info("End initialize input format state");
    }
}
