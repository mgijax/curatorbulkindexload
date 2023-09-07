#!/bin/sh
#
# Purpose:
#	wrapper for curatorbulkindexload.py
#
# History
#
# lec   09/06/2023
#	- wts2-1248/Bulk index associations (allele, marker, strain)
#

BINDIR=`dirname $0`
COMMON_CONFIG=`cd ${BINDIR}/..; pwd`/curatorbulkindexload.config
USAGE="Usage: curatorbulkindexload.sh"

#
# Make sure the common configuration file exists and source it.
#
if [ -f ${COMMON_CONFIG} ]
then
    . ${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

#
# Initialize the log file.
#
LOG=${LOGDIR}/curatorbulkindexload.sh.log
rm -rf ${LOG}
touch ${LOG}

#
# Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#####################################
#
# Main
#
#####################################

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#
preload ${OUTPUTDIR}

#
# if INPUT_FILE_DEFAULT does not exist, then skip load
#
if [ ! -f ${INPUT_FILE_DEFAULT} ]
then
        echo "Input file ${INPUT_FILE_DEFAULT} does not exist - skipping load" | tee -a ${LOG_PROC}
        # set STAT for shutdown
        STAT=0
        echo 'shutting down'
        shutDown
        exit 0
fi

# NOTE: keep this commented out until production release
#
# There should be a "lastrun" file in the input directory that was created
# the last time the load was run for this input file. If this file exists
# and is more recent than the input file, the load does not need to be run.
#
LASTRUN_FILE=${INPUTDIR}/lastrun
if [ -f ${LASTRUN_FILE} ]
then
    if test ${LASTRUN_FILE} -nt ${INPUT_FILE_DEFAULT}
    then
        echo "Input file has not been updated - skipping load" | tee -a ${LOG_PROC}
        # set STAT for shutdown
        STAT=0
        echo 'shutting down'
        shutDown
        exit 0
    fi
fi

echo "Running curator bulk index load" | tee -a ${LOG_DIAG}
${PYTHON} ${CURATORSTRAINLOAD}/bin/curatorbulkindexload.py ${INPUT_FILE_DEFAULT} load | tee -a ${LOG_DIAG}
STAT=$?
checkStatus ${STAT} "curatorbulkindexload.py"

#
# Touch the "lastrun" file to note when the load was run.
#
touch ${LASTRUN_FILE}

#
# run postload cleanup and email logs
#
shutDown

