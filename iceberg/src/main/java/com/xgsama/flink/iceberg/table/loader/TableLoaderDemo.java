package com.xgsama.flink.iceberg.table.loader;

import org.apache.iceberg.*;
import org.apache.iceberg.flink.TableLoader;

/**
 * TableLoaderDemo
 *
 * @author : xgSama
 * @date : 2022/2/17 23:30:03
 */
public class TableLoaderDemo {
    public static void main(String[] args) {

        TableLoader tableLoader = TableLoader.fromHadoopTable("file:///tmp/iceberg/warehouse/default_database/ds_users_sink");

        tableLoader.open();
        Table table = tableLoader.loadTable();

        Snapshot snapshot1 = table.currentSnapshot();

        TableScan tableScan = table.newScan();
        for (FileScanTask planFile : tableScan.planFiles()) {
            System.out.println(planFile);
        }

        Snapshot snapshot = tableScan.snapshot();
        System.out.println(snapshot.snapshotId());
        System.out.println(snapshot.manifestListLocation());
        for (DataFile addedFile : snapshot.addedFiles()) {
            System.out.println(addedFile);
        }

        System.out.println();
    }
}
