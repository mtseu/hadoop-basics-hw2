package com.epam.training.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.util.*;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.APPLICATION;

/**
 * Created by miket on 3/28/16.
 */
public class YClient {
    private static final String DEFAULT_STAGING_DIR="/opt/yarn/stage";
    private static final String DEFAULT_SRC_FILE="/opt/yarn/src-file";
    private static final int DEFAULT_CONTAINERS=3;

    private static final String SRC_FILE_ARG_NAME="src";
    private static final String CONTAINERS_NUMBER_ARG_NAME="containers";
    private static final String STAGING_DIR_ARG_NAME="stage";

    private static String stage = DEFAULT_STAGING_DIR;
    private static String src = DEFAULT_SRC_FILE;
    private static int containers=DEFAULT_CONTAINERS;

    /*
        arg[0] - source file location
        arg[1] - number of workers (containers)
        arg[2] - optional staging area
     */
    public static void main(String[] args) throws Exception {
        System.out.print("Initializing Yarn conf on client ...");
        YarnConfiguration yconf = new YarnConfiguration();
        System.out.println(" done");

        System.out.print("Parsing command line on client ...");
        GenericOptionsParser parser = new GenericOptionsParser(yconf, args);
        String[] arguments = parser.getRemainingArgs();
        for(String arg: arguments) {
            String[] oneArg = arg.split("=");
            if(oneArg.length != 2) {
                throw new Exception("Invalid argument:" + oneArg);
            }
            if(oneArg[0].equals(SRC_FILE_ARG_NAME)) {
                src = oneArg[1];
            } else if (oneArg[0].equals(STAGING_DIR_ARG_NAME)) {
                stage = oneArg[1];
            } else if (oneArg[0].equals(CONTAINERS_NUMBER_ARG_NAME)) {
                containers = Integer.parseInt(oneArg[1]);
            }
        }
        System.out.println(" done");

        System.out.println("Staging to:" + stage);
        System.out.println("Using source file from:" + src);
        System.out.println("Running containers:" + containers);


        System.out.print("Create yarn client...");
        YarnClient client = YarnClient.createYarnClient();
        System.out.println(" done");
        System.out.print("Init yarn client...");
        client.init(yconf);
        System.out.println(" done");
        System.out.print("Start yarn client...");
        client.start();
        System.out.println(" done");

        YarnClientApplication clientApp = client.createApplication();
        ApplicationSubmissionContext asc = clientApp.getApplicationSubmissionContext();

        asc.setApplicationName("y-app"); // application name
        asc.setAMContainerSpec(setupAMConteiner(yconf));
        asc.setResource(setupAMCaps());
        asc.setQueue("default"); // queue

        // Submit application
        ApplicationId appId = asc.getApplicationId();
        System.out.println("Submitting application " + appId);
        client.submitApplication(asc);

        ApplicationReport appReport = client.getApplicationReport(appId);

        System.out.println("AM Host:" + appReport.getHost());
        System.out.println("AM tracking URL:" + appReport.getTrackingUrl());
        System.out.println("AM original tracking URL:" + appReport.getOriginalTrackingUrl());
        System.out.println("AM rpc:" + appReport.getRpcPort());

        YarnApplicationState appState = appReport.getYarnApplicationState();
        while (appState != YarnApplicationState.FINISHED &&
                appState != YarnApplicationState.KILLED &&
                appState != YarnApplicationState.FAILED) {
            Thread.sleep(100);
            appReport = client.getApplicationReport(appId);
            appState = appReport.getYarnApplicationState();
        }
    }

    private static Resource setupAMCaps() {
        return Resource.newInstance(256, 1);
    }

    private static ContainerLaunchContext setupAMConteiner(YarnConfiguration conf) throws Exception {
        ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
                YUtil.setupAMJar(conf, stage, "hw2-yarn-client-1.0-SNAPSHOT.jar", true),
                YUtil.setupAMEnv(conf),
                setupAMCmd(), null, null, null);

        return clc;
    }

    private static List<String> setupAMCmd() {
        String cmd =
                "$JAVA_HOME/bin/java" +
                " -Xmx256M" +
                " com.epam.training.hw2.YAppMaster" +
                " " +  stage +
                " " + src +
                " " + containers +
                " 1> /tmp/ystdout_am.log" +
                " 2> /tmp/ystderr_am.log";

        System.out.println("Launching AM command:" + cmd);
        return Collections.singletonList(cmd);
    }

}
