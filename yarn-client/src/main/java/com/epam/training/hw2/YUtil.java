package com.epam.training.hw2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.yarn.api.records.LocalResourceType.FILE;
import static org.apache.hadoop.yarn.api.records.LocalResourceVisibility.APPLICATION;

/**
 * Created by miket on 3/30/16.
 */
class YUtil {
    static Map<String,String> setupAMEnv(Configuration conf) {
        Map<String, String> env = new HashMap<String, String>();

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(env,
                    ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(),
                    ApplicationConstants.CLASS_PATH_SEPARATOR);
        }

        Apps.addToEnvironment(env, ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator  + "hw2-yarn-client-1.0-SNAPSHOT.jar",
                ApplicationConstants.CLASS_PATH_SEPARATOR);

        return env;
    }

    static Map<String, LocalResource> setupAMJar(Configuration conf, String stageDir, String jar, boolean upload) throws Exception {
        FileSystem fs = FileSystem.get(conf);
        Path stagePath = new Path(stageDir);
        Path hdfsPath = new Path(stagePath, jar);
        URL hdfsURL = ConverterUtils.getYarnUrlFromPath(FileContext.getFileContext().makeQualified(hdfsPath));

        System.out.println("Staging path:" + hdfsPath);
        System.out.println("Staging URL:" + ConverterUtils.getYarnUrlFromPath(hdfsPath));

        if(upload) {
            fs.mkdirs(stagePath);
            fs.copyFromLocalFile(false, true,
                    new Path("hw2-yarn-client-1.0-SNAPSHOT.jar"),
                    hdfsPath);
        }


        FileStatus jarStat = fs.getFileStatus(hdfsPath);
        LocalResource lrc = LocalResource.newInstance(
                hdfsURL, FILE, APPLICATION,
                jarStat.getLen(), jarStat.getModificationTime());

        return Collections.singletonMap(jar, lrc);
    }

}
