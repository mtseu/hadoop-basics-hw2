## this is the second hw: sample yarn client


##Just run `./run.sh` and ignore all below

in order to build: `mvn package`


in order to run:
upload the source file to HDFS (the deafult location is "/opt/yarn" and name is "src-file"

cd yarn-client/target'
yarn jar hw2-yarn-client-1.0-SNAPSHOT.jar com.epam.training.hw2.YClient'


The following options are supported:

 `-src=hdfs-file`- file on hdfs to be processed
 `-containers=number` - number of containers to be launched
 `-stage=hdfs-dir` - HDFS staging directory (result file will be placed there)

