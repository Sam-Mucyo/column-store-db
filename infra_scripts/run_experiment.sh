#!/bin/bash

# JUST RUN THE SPECIFIED MILESTONE's EXPERIMENT (and pontentially test cases)

# Set base directory based on the location of this script, to all this script to work in both local and container
# environments. As long as this script is in `infra_scripts` directory, this will point to the project root directory.
BASE_DIR=$(dirname $(dirname $(realpath $0)))
INPUT_DIR=
OUTPUT_DIR=

# Input Parameters
UPTOMILE="${1:-5}"
PARAM_VAL="${2:-0}"
# Default wait time for server to recover data is 2 seconds.
WAIT_SECONDS_TO_RECOVER_DATA="${3:-2}"

if [ "$PARAM_VAL" -eq "0" ] ;
    then
        echo "Please provide a data size to run the experiment."
        exit 1
fi

EXPERIMENT_DATA_DIR="${BASE_DIR}/experiments/data"

# For M1 Experiment we add support for running tests for different data sizes in `EXPERIMENT/data/milestone1/n`
M1_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone1/${PARAM_VAL}"
M2_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone2/batch_size_${PARAM_VAL}"
M3_EXPERIMENT_DIR="${EXPERIMENT_DATA_DIR}/milestone3"

START_TEST=
MAX_TEST=65
TEST_IDS=`seq -w 1 ${MAX_TEST}`

if [ "$UPTOMILE" -eq "1" ] ;
then
    START_TEST=1
    MAX_TEST=9
    INPUT_DIR="${M1_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M1_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "2" ] ;
then
    # For experiments of m2, we only care about test 16-19 for batched queries.
    START_TEST=16
    MAX_TEST=19
    INPUT_DIR="${M2_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M2_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "3" ] ;
then
    # For experiments of m3, we only care about test 33-44 (performance tests)
    START_TEST=33
    MAX_TEST=44
    INPUT_DIR="${M3_EXPERIMENT_DIR}"
    OUTPUT_DIR="${M3_EXPERIMENT_DIR}"
elif [ "$UPTOMILE" -eq "4" ] ;
then
    MAX_TEST=59
elif [ "$UPTOMILE" -eq "5" ] ;
then
    MAX_TEST=65
fi

function killserver () {
    SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
    if [ $(($SERVER_NUM_RUNNING)) -ne 0 ]; then
        # kill any servers existing
        if pgrep server; then
            pkill -9 server
        fi
    fi
}

SERVER_RUN=0

for TEST_ID in $TEST_IDS
do
    if [ "$TEST_ID" -ge "$START_TEST" ] && [ "$TEST_ID" -le "$MAX_TEST" ] ;
    then
        if [ ${SERVER_RUN} -eq 0 ]
        then
            cd $BASE_DIR/src
            # start the server before the first case we test.
            SERVER_RUN=1
            ./server > last_server.out &
        elif [ "$UPTOMILE" -eq "1" ]
        then
            # Restart when running M1 Experiment so that all first select queries are run on fresh server.

            killserver

            # start the one server that should be serving test clients
            # invariant: at this point there should be NO servers running
            cd $BASE_DIR/src
            ./server > last_server.out &
            sleep $WAIT_SECONDS_TO_RECOVER_DATA
        fi
        SERVER_NUM_RUNNING=`ps aux | grep server | wc -l`
        if [ $(($SERVER_NUM_RUNNING)) -lt 1 ]; then
            echo "Warning: no server running at this point. Your server may have crashed early."
        fi

        $BASE_DIR/infra_scripts/run_test.sh $TEST_ID $INPUT_DIR $OUTPUT_DIR
        sleep 1
    fi
done

echo "Milestone Run is Complete up to Milestone #: $UPTOMILE"

killserver