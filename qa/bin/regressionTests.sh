REM Run from YADAMU_HOME
export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
source $YADAMU_QA_HOME/bin/export.sh
source $YADAMU_QA_HOME/bin/import.sh
source $YADAMU_QA_HOME/bin/dbRoundtrip.sh
source $YADAMU_QA_HOME/bin/fileRoundtrip.sh
