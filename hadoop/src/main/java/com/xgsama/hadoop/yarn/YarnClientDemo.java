package com.xgsama.hadoop.yarn;

import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

import java.util.EnumSet;
import java.util.List;

/**
 * YarnClientDemo
 *
 * @author : xgSama
 * @date : 2022/3/14 10:43:44
 */
public class YarnClientDemo {

    private static final YarnClient yarnClient;

    static {
        yarnClient = YarnClient.createYarnClient();
        YarnConfiguration configuration = new YarnConfiguration();
        yarnClient.init(configuration);
    }

    public static void main(String[] args) throws Exception {
        yarnClient.start();

        System.out.println("------");

        EnumSet<YarnApplicationState> states = EnumSet.noneOf(YarnApplicationState.class);
        states.add(YarnApplicationState.RUNNING);
//        states.add(YarnApplicationState.ACCEPTED);
        for (ApplicationReport application : yarnClient.getApplications(states)) {
            System.out.println(application.getApplicationId() + "->"
                    + application.getName() + "->"
                    + application.getApplicationType());
        }


        List<NodeReport> nodeReports = yarnClient.getNodeReports(NodeState.UNHEALTHY, NodeState.RUNNING);

        for (NodeReport nodeReport : nodeReports) {
            System.out.println(nodeReport.getNodeId() + " | "
                    + nodeReport.getUsed().getVirtualCores() + " | "
                    + nodeReport.getNodeState() + "|"
                    + nodeReport.getHealthReport().toLowerCase());
        }


        yarnClient.close();
    }
}
