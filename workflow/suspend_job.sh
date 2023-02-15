#!/usr/bin/env bash
#Suspend an oozie workflow

if [[ "$#" -ne 1 ]]; then
    echo "USAGE: ./suspend_job.sh WORKFLOW_ID"
    echo "WORKFLOW_ID is the Oozie Workflow ID, obtainable from the Hue GUI."
    echo "Example: ./suspend_job.sh 0000096-170207195912691-oozie-oozi-W"
    exit 1
fi

HOST=https://poldcdhen001.dev.intranet:11443/oozie
W_ID=$1

oozie job -oozie $HOST -suspend $W_ID
RC=$?
if [[ $RC -ne 0 ]]; then
    echo "Failed to suspend workflow, return code: $RC"
    exit 2
fi

exit 0
