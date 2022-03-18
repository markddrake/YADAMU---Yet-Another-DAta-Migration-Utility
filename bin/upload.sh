# Run from YADAMU_HOME
export YADAMU_HOME="${YADAMU_HOME:=`pwd`}"
if [ -z ${NODE_NO_WARNINGS+x} ]; then export NODE_NO_WARNINGS=1; fi
node $YADAMU_HOME/src/node/cli/upload.js "$@"