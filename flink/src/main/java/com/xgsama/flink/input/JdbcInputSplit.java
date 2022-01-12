package com.xgsama.flink.input;

import org.apache.flink.core.io.GenericInputSplit;

/**
 * JdbcInputSplit
 *
 * @author : xgSama
 * @date : 2022/1/10 15:46:47
 */
public class JdbcInputSplit extends GenericInputSplit {
    /**
     * Creates a generic input split with the given split number.
     *
     * @param partitionNumber         The number of the split's partition.
     * @param totalNumberOfPartitions The total number of the splits (partitions).
     */
    public JdbcInputSplit(
            int partitionNumber,
            int totalNumberOfPartitions,
            int mod,
            String startLocation,
            String endLocation,
            String startLocationOfSplit,
            String endLocationOfSplit) {
        super(partitionNumber, totalNumberOfPartitions);
        this.mod = mod;
        this.startLocation = startLocation;
        this.endLocation = endLocation;
        this.startLocationOfSplit = startLocationOfSplit;
        this.endLocationOfSplit = endLocationOfSplit;
    }


    private int mod;

    private String endLocation;

    private String startLocation;

    /**
     * 分片startLocation *
     */
    private String startLocationOfSplit;

    /**
     * 分片endLocation *
     */
    private String endLocationOfSplit;


    public int getMod() {
        return mod;
    }

    public void setMod(int mod) {
        this.mod = mod;
    }

    public String getEndLocation() {
        return endLocation;
    }

    public void setEndLocation(String endLocation) {
        this.endLocation = endLocation;
    }

    public String getStartLocation() {
        return startLocation;
    }

    public void setStartLocation(String startLocation) {
        this.startLocation = startLocation;
    }

    public String getStartLocationOfSplit() {
        return startLocationOfSplit;
    }

    public void setStartLocationOfSplit(String startLocationOfSplit) {
        this.startLocationOfSplit = startLocationOfSplit;
    }

    public String getEndLocationOfSplit() {
        return endLocationOfSplit;
    }

    public void setEndLocationOfSplit(String endLocationOfSplit) {
        this.endLocationOfSplit = endLocationOfSplit;
    }

    @Override
    public String toString() {
        return "JdbcInputSplit{"
                + "mod="
                + mod
                + ", endLocation='"
                + endLocation
                + '\''
                + ", startLocation='"
                + startLocation
                + '\''
                + ", startLocationOfSplit='"
                + startLocationOfSplit
                + '\''
                + ", endLocationOfSplit='"
                + endLocationOfSplit
                + '\''
                + '}';
    }
}
