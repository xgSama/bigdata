package com.xgsama.flink.submit;


import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.configuration.*;
import org.apache.flink.yarn.YarnClientYarnClusterInformationRetriever;
import org.apache.flink.yarn.YarnClusterDescriptor;
import org.apache.flink.yarn.YarnClusterInformationRetriever;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.util.Collections;

import static org.apache.flink.configuration.MemorySize.MemoryUnit.MEGA_BYTES;


/**
 * SubmitJobToYarn
 *
 * @author : xgSama
 * @date : 2022/3/14 16:46:00
 */
public class SubmitJobToYarn {

    private static final String confPathPrefix = "/Users/xgSama/IdeaProjects/bigdata/flink/src/main/resources/";

    public static void main(String[] args) {

        // flink的本地配置目录，为了得到flink的配置
        String configurationDirectory = "G:/flink_working_tools/yarn-clientconfig/yarn-conf";
        // 存放flink集群相关的jar包目录
        String flinkLibs = "hdfs://ns1/dtInsight/flink110/lib";
        String flinkDistJar = "hdfs://dev-ct6-dc-master01:8020/flink/flink-yarn_2.11-1.11.0.jar";

        // 用户jar
        String userJarPath = "hdfs://dev-ct6-dc-master01:8020/flink/SocketWindowWordCount.jar";


        YarnClient yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration entries = new org.apache.hadoop.conf.Configuration();
        entries.addResource(new Path(confPathPrefix + "yarn-site.xml"));
        entries.addResource(new Path(confPathPrefix + "hdfs-site.xml"));
        entries.addResource(new Path(confPathPrefix + "core-site.xml"));
        YarnConfiguration yarnConfiguration = new YarnConfiguration(entries);
        yarnClient.init(yarnConfiguration);
        yarnClient.start();



        YarnClusterInformationRetriever clusterInformationRetriever = YarnClientYarnClusterInformationRetriever
                .create(yarnClient);

        // 获取flink的配置
        Configuration flinkConfiguration = GlobalConfiguration.loadConfiguration(
                configurationDirectory);
        flinkConfiguration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
        flinkConfiguration.set(
                PipelineOptions.JARS,
                Collections.singletonList(
                        userJarPath));

        Path remoteLib = new Path(flinkLibs);
        flinkConfiguration.set(
                YarnConfigOptions.PROVIDED_LIB_DIRS,
                Collections.singletonList(remoteLib.toString()));
        flinkConfiguration.set(
                YarnConfigOptions.FLINK_DIST_JAR,
                flinkDistJar);

        // 设置为application模式
        flinkConfiguration.set(
                DeploymentOptions.TARGET,
                YarnDeploymentTarget.APPLICATION.getName());
        // yarn application name
        flinkConfiguration.set(YarnConfigOptions.APPLICATION_NAME, "TestApplication");

        flinkConfiguration.set(JobManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1024", MEGA_BYTES));

        ClusterSpecification clusterSpecification = new ClusterSpecification.ClusterSpecificationBuilder()
                .createClusterSpecification();


        // 设置用户jar的参数和主类
        ApplicationConfiguration appConfig = new ApplicationConfiguration(args, null);

        YarnClusterDescriptor yarnClusterDescriptor = new YarnClusterDescriptor(
                flinkConfiguration,
                yarnConfiguration,
                yarnClient,
                clusterInformationRetriever,
                true);
        ClusterClientProvider<ApplicationId> clusterClientProvider = null;
        try {
            clusterClientProvider = yarnClusterDescriptor.deployApplicationCluster(
                    clusterSpecification,
                    appConfig);
        } catch (ClusterDeploymentException e) {
            e.printStackTrace();
        }

        ClusterClient<ApplicationId> clusterClient = clusterClientProvider.getClusterClient();
        ApplicationId applicationId = clusterClient.getClusterId();
        System.out.println(applicationId);
    }
}
