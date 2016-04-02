package com.epam.training.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by miket on 3/29/16.
 */
public class YAppMaster implements AMRMClientAsync.CallbackHandler {
    Configuration conf;
    NMClient nm;
    AtomicInteger running;
    AtomicInteger started = new AtomicInteger(0);

    String stage;
    String src;
    int number;

    public YAppMaster(String stage, String src, int number) {
        this.number = number;
        this.stage = stage;
        this.src = src;

        running = new AtomicInteger(number);

        conf = new YarnConfiguration();
        nm = NMClient.createNMClient();
        nm.init(conf);
        nm.start();
    }


    public static void main(String[] args) throws Exception {
        YAppMaster master = new YAppMaster(args[0], args[1], Integer.valueOf(args[2]));
        master.run();
    }

    private void run() throws Exception {
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = AMRMClientAsync.createAMRMClientAsync(100, this);
        rmClient.init(conf);
        rmClient.start();

        // Register with ResourceManager
        try {
            System.out.print("RegisterApplicationMaster ...");
            rmClient.registerApplicationMaster("", 0, "");
            System.out.println("done");

            for (int c = 0; c < number; c++) {
                allocateContainer(rmClient);
            }

            while (running.get() > 0) {
                Thread.sleep(100);
            }

            merge();
        } finally {
            System.out.print("UnregisterApplicationMaster ...");
            rmClient.unregisterApplicationMaster(
                    FinalApplicationStatus.SUCCEEDED, "", "");
            System.out.println("done");
        }

    }

    private void merge () throws  Exception {
        FileSystem fs = FileSystem.get(conf);

        Path res = new Path(stage, "user.profile.tags.us.txt");
        try(FSDataOutputStream out = fs.create(res, true); PrintWriter writer = new PrintWriter(out)) {
            for (int i = 0; i < number; i++) {
                Path taggedN = new Path(stage, "tagged" + i + ".txt");
                if (fs.exists(taggedN)) {
                    System.out.print("Merging:" + taggedN);
                    try(FSDataInputStream in = fs.open(taggedN);
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in))) {
                        String line = reader.readLine();
                        while(line != null) {
                            writer.println(line);
                            line = reader.readLine();
                        }
                    }
                }
            }
        }
    }

    private void allocateContainer(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient) {
        ContainerRequest req = new ContainerRequest(
                Resource.newInstance(256, 1), null, null, Priority.newInstance(0));
        rmClient.addContainerRequest(req);
    }

    private ContainerLaunchContext setupLaunchContext(int n) throws Exception {
        ContainerLaunchContext clc = ContainerLaunchContext.newInstance(
            YUtil.setupAMJar(conf, stage, "hw2-yarn-client-1.0-SNAPSHOT.jar", false),
            YUtil.setupAMEnv(conf),
            Collections.singletonList(
                    "/bin/pwd > /tmp/yls_" + n + ".log; " +
                    "/bin/ls -R -la >> /tmp/yls_" + n + ".log; " +
                    "/bin/env >> /tmp/yls_" + n + ".log; " +
                    "$JAVA_HOME/bin/java -Xms128M -Xmx256M com.epam.training.hw2.YChild" +
                    " " + number +
                    " " + (n - 1) +
                    " " + src +
                    " " + stage +
                    " 1> /tmp/ystdout_container" + n + ".log" +
                    " 2> /tmp/ystderr_container" + n + ".log"),
            null, null, null);

        return clc;
    }

    public void onContainersCompleted(List<ContainerStatus> statuses) {
        for(ContainerStatus status: statuses) {
            System.out.println("Container:" + status.getContainerId() + " completed with exit status:" + status.getExitStatus());
            System.out.println("Container:" + status.getContainerId() + " diag:" + status.getDiagnostics());
            running.decrementAndGet();
        }
    }

    public void onContainersAllocated(List<Container> containers) {
        for(Container container: containers) {
            try {
                System.out.print("Starting container:" + container.getId());
                nm.startContainer(container, setupLaunchContext(started.incrementAndGet()));
                System.out.println("... done");
            } catch(Exception ex) {
                // TODO
                ex.printStackTrace(System.err);
                continue;
            }
        }
    }

    public void onShutdownRequest() {

    }

    public void onNodesUpdated(List<NodeReport> updatedNodes) {

    }

    public float getProgress() {
        return 0;
    }

    public void onError(Throwable e) {

    }
}
