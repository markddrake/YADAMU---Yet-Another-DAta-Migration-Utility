export YADAMU_TESTNAME=$1
if [ -z ${YADAMU_LOG_PATH+x} ]
then 
  if [ ! -e $YADAMU_LOG_ROOT ]; then mkdir $YADAMU_LOG_ROOT; fi
  if [ -z ${YADAMU_TIMESTAMP+x} ]; then export YADAMU_TIMESTAMP=$(date --utc +%FT%H%M%S.%3NZ); fi
  export YADAMU_LOG_PATH=$YADAMU_LOG_ROOT
  if [ ! -z ${YADAMU_LOG_PREFIX+x} ]; then export YADAMU_LOG_PATH=$YADAMU_LOG_PATH/$YADAMU_LOG_PREFIX; fi
  if [ ! -e $YADAMU_LOG_PATH ]; then mkdir $YADAMU_LOG_PATH; fi
  if [ ! -z ${YADAMU_TESTNAME+x} ]; then export YADAMU_LOG_PATH=$YADAMU_LOG_PATH/$YADAMU_TESTNAME; fi
  if [ ! -e $YADAMU_LOG_PATH ]; then mkdir $YADAMU_LOG_PATH; fi
  export YADAMU_LOG_PATH=$YADAMU_LOG_PATH/$YADAMU_TIMESTAMP;
fi
if [ ! -e $YADAMU_LOG_PATH ]; then mkdir $YADAMU_LOG_PATH; fi
export YADAMU_IMPORT_LOG=$YADAMU_LOG_PATH/yadamu.log
export YADAMU_EXPORT_LOG=$YADAMU_LOG_PATH/yadamu.log