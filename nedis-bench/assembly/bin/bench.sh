#!/bin/bash

JAVA_OPTS="-Xmx1g -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:CMSInitiatingOccupancyFraction=65 -XX:+CMSParallelRemarkEnabled"
JAVA=$JAVA_HOME/bin/java

function usage() {
    echo "Usage: bash $0 nedis|jedis command"
    echo "command:"
    echo "    start"
    echo "    stop"
    exit 1
}

if [ $# -lt 2 ]; then
    usage
fi

if [ "$1" == nedis ]; then
    MAINCLASS="io.codis.nedis.bench.NedisBench"
    OUT_FILE=nedis.out.$(date +%y%m%d-%H%M%S)
else
    MAINCLASS="io.codis.nedis.bench.JedisBench"
    OUT_FILE=jedis.out.$(date +%y%m%d-%H%M%S)
fi

COMMAND=$2
shift 2

if [ ${COMMAND} == "start" ];then
    # add libs to CLASSPATH
    for f in lib/*.jar; do
        CLASSPATH=${CLASSPATH}:$f;
    done
    export CLASSPATH
    nohup ${JAVA} ${JAVA_OPTS} ${MAINCLASS} &>${OUT_FILE} $@ &
    sleep 3
    echo
    # show some logs by tail
    tail -n 10 ${OUT_FILE}
fi

if [ ${COMMAND} == "stop" ];then
    PIDS=`ps x | grep ${MAINCLASS} | grep -v grep | grep java | awk '{print $1}'`
    for PID in ${PIDS};do
        kill -9 ${PID}
        echo "kill process from pid: ${PID}"
    done
fi
