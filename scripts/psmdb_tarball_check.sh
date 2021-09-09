#!/usr/bin/env bash
set -e
export CLASSPATH=$CLASSPATH:/workdir/mongo-java-driver.jar
ulimit -c unlimited

Y_OPERATIONS=${Y_OPERATIONS:-1000000}
S_DURATION=${S_DURATION:-5}
S_COLLECTIONS=${S_COLLECTIONS:-1}
B_DOCSPERCOL=${B_DOCSPERCOL:-1000000}
S_WRITE_CONCERN="${S_WRITE_CONCERN:-SAFE}"
B_LOADER_THREADS=${B_LOADER_THREADS:-4}
B_WRITER_THREADS=${B_WRITER_THREADS:-4}

cd /workdir
echo "Configuring sysbench"
sed -i "/export DB_NAME=/c\export DB_NAME=bench_test" sysbench-mongodb/config.bash
sed -i "/export USERNAME=/c\export USERNAME=none" sysbench-mongodb/config.bash
sed -i "/export NUM_COLLECTIONS=/c\export NUM_COLLECTIONS=${S_COLLECTIONS}" sysbench-mongodb/config.bash
sed -i "/export NUM_DOCUMENTS_PER_COLLECTION=/c\export NUM_DOCUMENTS_PER_COLLECTION=${B_DOCSPERCOL}" sysbench-mongodb/config.bash
sed -i "/export RUN_TIME_MINUTES=/c\export RUN_TIME_MINUTES=${S_DURATION}" sysbench-mongodb/config.bash
sed -i "/export DROP_COLLECTIONS=/c\export DROP_COLLECTIONS=FALSE" sysbench-mongodb/config.bash
sed -i "/export NUM_WRITER_THREADS=/c\export NUM_WRITER_THREADS=${B_WRITER_THREADS}" sysbench-mongodb/config.bash
sed -i "/export NUM_LOADER_THREADS=/c\export NUM_LOADER_THREADS=${B_LOADER_THREADS}" sysbench-mongodb/config.bash
sed -i "/export WRITE_CONCERN=/c\export WRITE_CONCERN=${S_WRITE_CONCERN}" sysbench-mongodb/config.bash

echo "Executing sysbench"
pushd sysbench-mongodb
./run.simple.bash
popd
echo "Finished sysbench"

echo "Executing ycsb"
pushd ycsb-mongodb-binding
./bin/ycsb load mongodb -s -P workloads/workloadb -p recordcount=${B_DOCSPERCOL} -threads ${B_LOADER_THREADS} -p mongodb.auth="false" > ycsb-load.txt
./bin/ycsb.sh run mongodb -s -P workloads/workloadb -p recordcount=${B_DOCSPERCOL} -p operationcount=${Y_OPERATIONS} -threads ${B_WRITER_THREADS} -p mongodb.auth="false" > ycsb-run.txt
popd
echo "Finished ycsb"

echo "Finished with bench tools"
