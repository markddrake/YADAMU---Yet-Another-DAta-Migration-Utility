# Run from YADAMU_HOME
export YADAMU_HOME=`pwd`
if [ -z ${NODE_NO_WARNINGS+x} ]; then export NODE_NO_WARNINGS=1; fi
if [ -z ${NODE_OPTIONS+x} ]; then export NODE_OPTIONS="--max_old_space_size=8192"; fi
node $YADAMU_HOME/src/qa/cli/test.js CONFIG=$1