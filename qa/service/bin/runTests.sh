#!/bin/bash
TESTSUITE_START_TIME=`date +%s`
if [ -z ${1+x} ]; then export YADAMU_SETTINGS=default; else export YADAMU_SETTINGS=$1; fi
export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
unset YADAMU_TIMESTAMP
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}") 
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
export YADAMU_TESTNAME=service
export YADAMU_OUTPUT_PATH=$YADAMU_HOME/mnt/$YADAMU_TESTNAME
mkdir -p $YADAMU_OUTPUT_PATH
source $YADAMU_HOME/qa/bin/initializeLogging.sh $YADAMU_TESTNAME
export SERVICE_LOG_FILE=$YADAMU_LOG_PATH/service.log
export LOG_SEPERATOR="+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+"
echo  $LOG_SEPERATOR > $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/about.log                                             http://YADAMU-SVC:3000/yadamu/about
cat $YADAMU_LOG_PATH/about.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/config.log -T qa/regression/serviceConfiguration.json http://YADAMU-SVC:3000/yadamu/configuration
cat $YADAMU_LOG_PATH/config.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_OUTPUT_PATH/HR.json                                            http://YADAMU-SVC:3000/yadamu/download/source/oracle19c/schema/HR
cat $YADAMU_OUTPUT_PATH/HR.json >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/export.log                                            http://YADAMU-SVC:3000/yadamu/export/source/oracle19c/schema/HR/directory/stagingArea/file/HR.json
cat $YADAMU_LOG_PATH/export.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/upload.log -T $YADAMU_OUTPUT_PATH/HR.json             http://YADAMU-SVC:3000/yadamu/upload/target/oracle19c/schema/HR1
cat $YADAMU_LOG_PATH/upload.log  >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/import.log                                            http://YADAMU-SVC:3000/yadamu/import/directory/stagingArea/file/HR.json/target/oracle19c/schema/HR1
cat $YADAMU_LOG_PATH/import.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/copy.log                                              http://YADAMU-SVC:3000/yadamu/copy/source/oracle19c/schema/HR/target/postgres%231/schema/HR1
cat $YADAMU_LOG_PATH/copy.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/copy.log                                              http://YADAMU-SVC:3000/yadamu/copy/source/oracle18c/schema/FOO/target/postgres%232/schema/BAA
cat $YADAMU_LOG_PATH/copy.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/copy.log                                              http://YADAMU-SVC:3000/yadamu/copy/source/oracle19c/schema/FOO/target/postgres%232/schema/BAA
cat $YADAMU_LOG_PATH/copy.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
curl -o $YADAMU_LOG_PATH/copy.log                                              http://YADAMU-SVC:3000/yadamu/copy/source/oracle19c/schema/FOO/target/postgres%231/schema/BAA
cat $YADAMU_LOG_PATH/copy.log >> $SERVICE_LOG_FILE
echo  -e "\n$LOG_SEPERATOR" >> $SERVICE_LOG_FILE
END_TIME=`date +%s`
TOTAL_TIME=$((END_TIME-TESTSUITE_START_TIME))
ELAPSED_TIME=`date -d@$TOTAL_TIME -u +%H:%M:%S`
echo "Test Suite Elapsed time: ${ELAPSED_TIME}."  >> $SERVICE_LOG_FILE
echo "Test Suite Elapsed time: ${ELAPSED_TIME}."
