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
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh uploadRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
  ;;	
  
  everything)
    source qa/bin/createOutputFolders.sh mnt
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh initialize
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageDataSets
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11ShortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeDataTypes
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
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014TestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeTestSuite
    source $YADAMU_SCRIPT_DIR/runCmdLineTests.sh
    ;;	
  
  oracle21c)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cTestSuite
  ;;

  oracle11g)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gTestSuite
  ;;

  db2)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2TestSuite
  ;;

  snowflake)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
  ;;

  vertica11)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
  ;;
    
  vertica10)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
  ;;

  # source $YADAMU_SCRIPT_DIR/runRegressionTest.sh redshiftCopy

  vertica09)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica09Copy
  ;;

  ydb)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh ydbShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh ydbDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh ydbTestSuite
  ;;

  copy) 
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
  ;;

  cmdLine) 
    source  $YADAMU_SCRIPT_DIR/runCmdLineTests.sh
  ;;

  custom)
    source $YADAMU_SCRIPT_DIR/runCustomTest.sh $TESTNAME
  ;; 
  
  regression)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh $TESTNAME
  ;; 

  interactive)
    sleep 365d
  ;; 
  
  
  *)
    echo "Invalid Test $YADAMU_TEST_NAME: Valid values are shortRegression, export, snowflake, vertica, vertica09, cmdLine, copy, interactive or custom"
  ;;
esac