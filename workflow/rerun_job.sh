#!/usr/bin/env bash
#Rerun an oozie workflow, restarting any failed jobs

if [[ "$#" -ne 1 ]]; then
    echo "USAGE: ./rurun_job.sh WORKFLOW_ID"
    echo "WORKFLOW_ID is the Oozie Workflow ID, obtainable from the Hue GUI."
    echo "Example: ./rerun_job.sh 0000096-170207195912691-oozie-oozi-W"
    exit 1
fi

HOST=https://poldcdhen001.dev.intranet:11443/oozie
W_ID=$1

oozie job -oozie $HOST -rerun $W_ID -Doozie.wf.rerun.failnodes=true
RC=$?
if [[ $RC -ne 0 ]]; then
    echo "Failed to restart workflow, return code: $RC"
    exit 2
fi

exit 0
