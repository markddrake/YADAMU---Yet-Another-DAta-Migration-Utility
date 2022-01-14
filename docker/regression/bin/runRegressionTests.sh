export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source qa/bin/createOutputFolders.sh mnt
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh uploadRoundtrip
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
