if [ ! -e $YADAMU_LOG_ROOT ]; then mkdir $YADAMU_LOG_ROOT; fi
if [ -z ${YADAMU_LOG_PATH+x} ]; then export YADAMU_LOG_PATH=$YADAMU_LOG_ROOT/$(date --utc +%FT%T.%3NZ); fi
if [ ! -e $YADAMU_LOG_PATH ]; then mkdir $YADAMU_LOG_PATH; fi
if [ ! -e $YADAMU_LOG_PATH/install ]; then mkdir $YADAMU_LOG_PATH/install; fi
export IMPORTLOG=$YADAMU_LOG_PATH/yadamu.log
export EXPORTLOG=$YADAMU_LOG_PATH/yadamu.log