#!/usr/bin/env bash
#Resume an oozie workflow

if [[ "$#" -ne 1 ]]; then
    echo "USAGE: ./resume_job.sh WORKFLOW_ID"
    echo "WORKFLOW_ID is the Oozie Workflow ID, obtainable from the Hue GUI."
    echo "Example: ./resume_job.sh 0000096-170207195912691-oozie-oozi-W"
    exit 1
fi

HOST=https://poldcdhen001.dev.intranet:11443/oozie
W_ID=$1

oozie job -oozie $HOST -resume $W_ID
RC=$?
if [[ $RC -ne 0 ]]; then
    echo "Failed to resume workflow, return code: $RC"
    exit 2
fi

exit 0
