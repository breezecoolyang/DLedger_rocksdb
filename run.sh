#!/bin/bash

function usage()
{
    echo "usage: run.sh <command> [<args>]"
    echo
    echo "Command list:"
    echo "   start                    start the cluster"
    echo "   stop                     stop the cluster"
}


######################
## start_cluster
#####################
function start_run()
{
    echo "start the cluster:"
    nohup java -jar target/DLedger.jar server -i "n0" -p "n0-localhost:40911;n1-localhost:40912;n2-localhost:40913" &
    nohup java -jar target/DLedger.jar server -i "n1" -p "n0-localhost:40911;n1-localhost:40912;n2-localhost:40913" &
    nohup java -jar target/DLedger.jar server -i "n2" -p "n0-localhost:40911;n1-localhost:40912;n2-localhost:40913" &
}

####################
## stop_cluster
#####################
function stop_run()
{
    echo "stop the clusetr:"
    ps -ef | grep "DLedger.jar" | grep -v "grep" | awk '{print $2}' | xargs kill -9
}


####################################################################

if [ $# -eq 0 ]; then
    usage
    exit 0
fi
cmd=$1
case $cmd in
    help)
        usage
        ;;
    start)
        shift
        start_run $*
        ;;
    stop)
        shift
        stop_run $*
        ;;
        *)
        echo "ERROR: unknown command $cmd"
        echo
        usage
        exit 1
esac

