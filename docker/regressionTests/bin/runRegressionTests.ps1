$ENV:YADAMU_HOME = Get-Location
$ENV:YADAMU_QA_HOME=$ENV:YADAMU_HOME/qa
$ENV:YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source qa/bin/createOutputFolders.sh mnt
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh export
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh import
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
source $ENV:YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
