package com.xgsama.flink.input;

import com.xgsama.flink.model.SourceJobConf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * MyJdbcInputFormat
 *
 * @author : xgSama
 * @date : 2022/1/10 11:59:47
 */
@Slf4j
public class MyJdbcInputFormat extends RichInputFormat<Row, InputSplit> {

    SourceJobConf conf;

    public MyJdbcInputFormat(SourceJobConf conf) {
        this.conf = conf;
    }


    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        try {
            return createInputSplitsInternal(minNumSplits);
        } catch (Exception e) {
            log.warn("error to create InputSplits", e);
            return new InputSplit[0];
        }

    }

    protected InputSplit[] createInputSplitsInternal(int minNumSplits) throws Exception {
        return createSplitsInternalBySplitRange(minNumSplits);
    }

    protected JdbcInputSplit[] createSplitsInternalBySplitRange(int minNumSplits) {
        JdbcInputSplit[] splits;
        Pair<String, String> splitRangeFromDb = getSplitRangeFromDb();
        BigInteger left = NumberUtils.createBigInteger(splitRangeFromDb.getLeft());
        BigInteger right = NumberUtils.createBigInteger(splitRangeFromDb.getRight());
        log.info("create splitsInternal,the splitKey range is {} --> {}", left, right);
        // 没有数据 返回空数组
        if (left == null || right == null) {
            splits = new JdbcInputSplit[minNumSplits];
        } else {
            BigInteger endAndStartGap = right.subtract(left);

            BigInteger step = endAndStartGap.divide(BigInteger.valueOf(minNumSplits));
            BigInteger remainder = endAndStartGap.remainder(BigInteger.valueOf(minNumSplits));
            if (step.compareTo(BigInteger.ZERO) == 0) {
                // left = right时，step和remainder都为0
                if (remainder.compareTo(BigInteger.ZERO) == 0) {
                    minNumSplits = 1;
                } else {
                    minNumSplits = remainder.intValue();
                }
            }

            splits = new JdbcInputSplit[minNumSplits];
            BigInteger start;
            BigInteger end = left;
            for (int i = 0; i < minNumSplits; i++) {
                start = end;
                end = start.add(step);
                end =
                        end.add(
                                (remainder.compareTo(BigInteger.valueOf(i)) > 0)
                                        ? BigInteger.ONE
                                        : BigInteger.ZERO);
                // 分片范围是 splitPk >=start and splitPk < end 最后一个分片范围是splitPk >= start
                if (i == minNumSplits - 1) {
                    end = null;
                }
                splits[i] =
                        new JdbcInputSplit(
                                i,
                                minNumSplits,
                                i,
                                null,
                                null,
                                start.toString(),
                                Objects.isNull(end) ? null : end.toString());
            }
        }
        return splits;
    }

    private Pair<String, String> getSplitRangeFromDb() {

        Pair<String, String> splitPkRange = null;
        Connection conn = null;
        Statement st = null;
        ResultSet rs = null;

        try {
            long startTime = System.currentTimeMillis();

            String querySplitRangeSql = buildQuerySplitRangeSql();
            log.info(String.format("Query SplitRange sql is '%s'", querySplitRangeSql));

            conn = getConnection();
            st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            st.setQueryTimeout(100);
            rs = st.executeQuery(querySplitRangeSql);
            if (rs.next()) {
                splitPkRange =
                        Pair.of(
                                String.valueOf(rs.getObject("min_value")),
                                String.valueOf(rs.getObject("max_value")));
            }

            log.info(
                    String.format(
                            "Takes [%s] milliseconds to get the SplitRange value [%s]",
                            System.currentTimeMillis() - startTime, splitPkRange));

            return splitPkRange;
        } catch (Throwable e) {
            log.error(e.getMessage());
            throw new RuntimeException(e.getMessage());
        } finally {
            closeDbResources(rs, st, conn, false);
        }
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


    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return null;
    }


    public String buildQuerySplitRangeSql() {
        String splitKey = conf.getSplitKey();
        String tableName = conf.getTableName();
        String whereFilter = conf.getWhereFilter();

        String format = String.format("SELECT max(%s.%s) as max_value, min(%s.%s) as min_value FROM %s %s",
                tableName, "\"" + splitKey + "\"",
                tableName, "\"" + splitKey + "\"",
                tableName, whereFilter);

        return format;
    }

    public Connection getConnection() {
        return null;
    }

    public void closeDbResources(ResultSet rs, Statement stmt, Connection conn, boolean commit) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.warn("Close resultSet error: { }", e);
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.warn("Close statement error:{ }", e);
            }
        }

        if (null != conn) {
            try {


                conn.close();
            } catch (SQLException e) {
                log.warn("Close connection error:{ }", e);
            }
        }
    }
}
