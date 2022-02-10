package com.xgsama.flink.util;

import org.apache.flink.api.common.io.DefaultInputSplitAssigner;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Arrays;

/**
 * JdbcInputFormat
 *
 * @author xgSama
 * @date 2021/4/19 14:25
 */
public class JdbcInputFormat extends RichInputFormat<Row, InputSplit> implements ResultTypeQueryable<Row> {

    protected static final Logger log = LoggerFactory.getLogger(JdbcInputFormat.class);

    protected String username;
    protected String password;
    protected String driverName;
    protected String dbURL;
    protected String queryTemplate;
    protected int resultSetType;
    protected int resultSetConcurrency;
    protected RowTypeInfo rowTypeInfo;

    protected transient Connection dbConn;
    protected transient PreparedStatement statement;
    protected transient ResultSet resultSet;
    protected int fetchSize;
    // Boolean to distinguish between default value and explicitly set autoCommit mode.
    protected Boolean autoCommit;

    protected boolean hasNext;
    protected Object[][] parameterValues;

    public JdbcInputFormat() {
    }

    @Override
    public void configure(Configuration parameters) {

    }

    @Override
    public void openInputFormat() throws IOException {
        //called once per inputFormat (on open)
        try {
            Class.forName(driverName);
            if (username == null) {
                dbConn = DriverManager.getConnection(dbURL);
            } else {
                dbConn = DriverManager.getConnection(dbURL, username, password);
            }

            // set autoCommit mode only if it was explicitly configured.
            // keep connection default otherwise.
            if (autoCommit != null) {
                dbConn.setAutoCommit(autoCommit);
            }

            statement = dbConn.prepareStatement(queryTemplate, resultSetType, resultSetConcurrency);
            if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        } catch (ClassNotFoundException cnfe) {
            throw new IllegalArgumentException("JDBC-Class not found. - " + cnfe.getMessage(), cnfe);
        }
    }

    @Override
    public void closeInputFormat() throws IOException {
        //called once per inputFormat (on close)
        try {
            if (statement != null) {
                statement.close();
            }
        } catch (SQLException se) {
            log.info("Inputformat Statement couldn't be closed - " + se.getMessage());
        } finally {
            statement = null;
        }

        try {
            if (dbConn != null) {
                dbConn.close();
            }
        } catch (SQLException se) {
            log.info("Inputformat couldn't be closed - " + se.getMessage());
        } finally {
            dbConn = null;
        }

        parameterValues = null;
    }

    @Override
    public BaseStatistics getStatistics(BaseStatistics cachedStatistics) throws IOException {
        return cachedStatistics;
    }

    @Override
    public InputSplit[] createInputSplits(int minNumSplits) throws IOException {
        if (parameterValues == null) {
            return new GenericInputSplit[]{new GenericInputSplit(0, 1)};
        }
        GenericInputSplit[] ret = new GenericInputSplit[parameterValues.length];
        for (int i = 0; i < ret.length; i++) {
            ret[i] = new GenericInputSplit(i, ret.length);
        }
        return ret;
    }

    @Override
    public InputSplitAssigner getInputSplitAssigner(InputSplit[] inputSplits) {
        return new DefaultInputSplitAssigner(inputSplits);
    }


    @Override
    public void open(InputSplit inputSplit) throws IOException {
        try {
            if (inputSplit != null && parameterValues != null) {
                for (int i = 0; i < parameterValues[inputSplit.getSplitNumber()].length; i++) {
                    Object param = parameterValues[inputSplit.getSplitNumber()][i];
                    if (param instanceof String) {
                        statement.setString(i + 1, (String) param);
                    } else if (param instanceof Long) {
                        statement.setLong(i + 1, (Long) param);
                    } else if (param instanceof Integer) {
                        statement.setInt(i + 1, (Integer) param);
                    } else if (param instanceof Double) {
                        statement.setDouble(i + 1, (Double) param);
                    } else if (param instanceof Boolean) {
                        statement.setBoolean(i + 1, (Boolean) param);
                    } else if (param instanceof Float) {
                        statement.setFloat(i + 1, (Float) param);
                    } else if (param instanceof BigDecimal) {
                        statement.setBigDecimal(i + 1, (BigDecimal) param);
                    } else if (param instanceof Byte) {
                        statement.setByte(i + 1, (Byte) param);
                    } else if (param instanceof Short) {
                        statement.setShort(i + 1, (Short) param);
                    } else if (param instanceof Date) {
                        statement.setDate(i + 1, (Date) param);
                    } else if (param instanceof Time) {
                        statement.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        statement.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof Array) {
                        statement.setArray(i + 1, (Array) param);
                    } else {
                        //extends with other types if needed
                        throw new IllegalArgumentException("open() failed. Parameter " + i + " of type " + param.getClass() + " is not handled (yet).");
                    }
                }
                if (log.isDebugEnabled()) {
                    log.debug(String.format("Executing '%s' with parameters %s", queryTemplate, Arrays.deepToString(parameterValues[inputSplit.getSplitNumber()])));
                }
            }
            resultSet = statement.executeQuery();
            hasNext = resultSet.next();
        } catch (SQLException se) {
            throw new IllegalArgumentException("open() failed." + se.getMessage(), se);
        }
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !hasNext;
    }

    @Override
    public Row nextRecord(Row reuse) throws IOException {
        try {
            if (!hasNext) {
                return null;
            }
            for (int pos = 0; pos < reuse.getArity(); pos++) {
                reuse.setField(pos, resultSet.getObject(pos + 1));
            }
            //update hasNext after we've read the record
            hasNext = resultSet.next();
            return reuse;
        } catch (SQLException se) {
            throw new IOException("Couldn't read data - " + se.getMessage(), se);
        } catch (NullPointerException npe) {
            throw new IOException("Couldn't access resultSet", npe);
        }
    }

    @Override
    public void close() throws IOException {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (SQLException se) {
            log.info("Inputformat ResultSet couldn't be closed - " + se.getMessage());
        }
    }

    @Override
    public TypeInformation<Row> getProducedType() {
        return rowTypeInfo;
    }

    protected PreparedStatement getStatement() {
        return statement;
    }

    protected Connection getDbConn() {
        return dbConn;
    }

    public static JdbcInputFormatBuilder buildJdbcInputFormat() {
        return new JdbcInputFormatBuilder();
    }


    public static class JdbcInputFormatBuilder {

        private final JdbcInputFormat format;

        public JdbcInputFormatBuilder() {
            this.format = new JdbcInputFormat();
            //using TYPE_FORWARD_ONLY for high performance reads
            this.format.resultSetType = ResultSet.TYPE_FORWARD_ONLY;
            this.format.resultSetConcurrency = ResultSet.CONCUR_READ_ONLY;
        }

        public JdbcInputFormatBuilder setUsername(String username) {
            format.username = username;
            return this;
        }

        public JdbcInputFormatBuilder setPassword(String password) {
            format.password = password;
            return this;
        }

        public JdbcInputFormatBuilder setDrivername(String driveName) {
            format.driverName = driveName;
            return this;
        }

        public JdbcInputFormatBuilder setDBUrl(String dbURL) {
            format.dbURL = dbURL;
            return this;
        }

        public JdbcInputFormatBuilder setQuery(String query) {
            format.queryTemplate = query;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetType(int resultSetType) {
            format.resultSetType = resultSetType;
            return this;
        }

        public JdbcInputFormatBuilder setResultSetConcurrency(int resultSetConcurrency) {
            format.resultSetConcurrency = resultSetConcurrency;
            return this;
        }

//        public JdbcInputFormatBuilder setParametersProvider(JdbcParameterValuesProvider parameterValuesProvider) {
//            format.parameterValues = parameterValuesProvider.getParameterValues();
//            return this;
//        }

        public JdbcInputFormatBuilder setRowTypeInfo(RowTypeInfo rowTypeInfo) {
            format.rowTypeInfo = rowTypeInfo;
            return this;
        }

        public JdbcInputFormatBuilder setFetchSize(int fetchSize) {
            Preconditions.checkArgument(fetchSize == Integer.MIN_VALUE || fetchSize > 0,
                    "Illegal value %s for fetchSize, has to be positive or Integer.MIN_VALUE.", fetchSize);
            format.fetchSize = fetchSize;
            return this;
        }

        public JdbcInputFormatBuilder setAutoCommit(Boolean autoCommit) {
            format.autoCommit = autoCommit;
            return this;
        }

        public JdbcInputFormat finish() {
            if (format.username == null) {
                log.info("Username was not supplied separately.");
            }
            if (format.password == null) {
                log.info("Password was not supplied separately.");
            }
            if (format.dbURL == null) {
                throw new IllegalArgumentException("No database URL supplied");
            }
            if (format.queryTemplate == null) {
                throw new IllegalArgumentException("No query supplied");
            }
            if (format.driverName == null) {
                throw new IllegalArgumentException("No driver supplied");
            }
            if (format.rowTypeInfo == null) {
                throw new IllegalArgumentException("No " + RowTypeInfo.class.getSimpleName() + " supplied");
            }
            if (format.parameterValues == null) {
                log.debug("No input splitting configured (data will be read with parallelism 1).");
            }
            return format;
        }

    }

}
