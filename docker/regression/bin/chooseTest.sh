export YADAMU_HOME="${YADAMU_HOME:=`pwd`}"
export YADAMU_QA_HOME=$YADAMU_HOME/qa
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
export YADAMU_TEST_NAME=${YADAMU_TEST_NAME:-all}
source qa/bin/createSharedFolders.sh mnt
case $YADAMU_TEST_NAME  in

  shortRegression)
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/postgres
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
  ;;
  
  export)
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/mysql
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/mssql
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/oracle
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
  ;;
  
  all)
    source qa/bin/createOutputFolders.sh mnt
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
  ;;	
  
  snowflake)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeTestSuite
  ;;

  vertica)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
  ;;
    
  cmdLine) 
    source  $YADAMU_SCRIPT_DIR/runCmdLineTests.sh
  ;;

  copy) 
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
  ;;
	# source $YADAMU_SCRIPT_DIR/runRegressionTest.sh redshiftCopy

  custom)
    source $YADAMU_SCRIPT_DIR/runCustomTest.sh $TESTNAME
  ;; 
  
  regression)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh $TESTNAME
  ;; 

  vertica09)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09Copy
  ;;

  interactive)
    sleep 365d
  ;; 
  
  *)
    echo "Invalid Test $YADAMU_TEST_NAME: Valid values are shortRegression, export, snowflake, vertica, vertica09, cmdLine, copy, interactive or custom"
  ;;
esac