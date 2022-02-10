package com.xgsama.flink.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.Tuple2;

import java.util.List;

/**
 * OperatorStateDemo
 *
 * @author : xgSama
 * @date : 2022/2/10 17:02:04
 */
public class OperatorStateDemo {

    public static void main(String[] args) {

    }

    static class BufferingSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {

        private final int threshold;
        private transient ListState<Tuple2<String, Integer>> checkpointedState;
        private List<Tuple2<String, Integer>> bufferElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            bufferElements.add(value);
            if (bufferElements.size() == threshold) {
                for (Tuple2<String, Integer> bufferElement : bufferElements) {
                    System.out.println(bufferElement);
                }
                bufferElements.clear();
            }
        }

        // Operator级别的State需要用户来实现快照保存逻辑
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            for (Tuple2<String, Integer> bufferElement : bufferElements) {
                checkpointedState.add(bufferElement);
            }
        }

        // Operator级别的State需要用户来实现状态的初始化
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                    "buffered-elements",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                    })
            );
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            if (context.isRestored()) {
                for (Tuple2<String, Integer> element : checkpointedState.get()) {
                    bufferElements.add(element);
                }
            }
        }
    }
}
