export YADAMU_HOME="${YADAMU_HOME:=`pwd`}"
export YADAMU_QA_HOME=$YADAMU_HOME/qa
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
export YADAMU_TEST_NAME=${YADAMU_TEST_NAME:-all}
source qa/bin/createSharedFolders.sh mnt
case $YADAMU_TEST_NAME  in

  initialization)
    source qa/bin/createOutputFolders.sh mnt
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh initialize
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageDataSets
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteStageDataSets
  ;;
  
  standard)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaShortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10ShortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDDL
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageCSVDataSets	
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014TestSuite
  ;;
	
  shortRegression)
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/postgres
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
  ;;
  
  reset)
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/mysql
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/mssql
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/oracle
    source qa/bin/resetFolders.sh mnt/stagingArea/export/json/postgres
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh initialize
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageDataSets
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh importDataTypes
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
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDDL
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
  
  everything)
    source qa/bin/createOutputFolders.sh mnt
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh initialize
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageDataSets
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh shortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaShortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10ShortRegression
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDataTypes
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresDDL
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageCSVDataSets	
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014TestSuite
    ;;	
  
  upload)
    # Server-side upload (parser SQL) is frozen: crash fixes only. It isn't part of standard or extended. The orchestrator's
    # upload test runs this set on the standing stack, then each oracle<version>Upload set with that version's container up.
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh uploadRoundtrip
  ;;

  oracle19cUpload|oracle21cUpload|oracle23aiUpload|oracle26aiUpload)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh $YADAMU_TEST_NAME
  ;;

  loader)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh loaderTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh awsTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh azureTestSuite
  ;;
  
  oracle)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oraclecCopy
  ;;

  oracle23ai)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle23aiShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle23aiDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle23aiTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle23aiLostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle23aiCopy
  ;;

  oracle26ai)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle26aiShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle26aiDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle26aiTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle26aiLostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle26aiCopy
  ;;

  oracle21c)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cLostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle21cCopy
  ;;

  oracle19c)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle19cShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle19cDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle19cTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle19cLostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle19cCopy
  ;;

  oracle11g)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gTestSuite
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gLostConnection
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracle11gCopy
  ;;

  mssql)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssqlShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssqlDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssqlTestSuite
  ;;

  mssql19)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2019ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2019DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2019TestSuite
  ;;

  mssql17)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2017ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2017DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2017TestSuite
  ;;

  mssql14)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2014TestSuite
  ;;

  mariadb)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
  ;;

  mssql12)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2012ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2012DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mssql2012TestSuite
  ;;

  sqlite)
    # The source schemas are built by sqliteStageDataSets during initialization.
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteAsTargetRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteAsSourceRoundtrip
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteMultiSchema
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh sqliteParallelRoundtrip
  ;;

  db2)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh db2TestSuite
  ;;

  teradata)
    # Needs the yadamu/teradata image (the driver isn't in the base images) and TDATA17-01 running on yadamu-db3.
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh teradataShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh teradataDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh teradataTestSuite
  ;;

  snowflake)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeStageCSVDataSets
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeCopy
  ;;

  vertica)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaTestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaCopy
  ;;

  mongo)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoTestSuite
  ;;

  vertica12)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica12ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica12DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica12TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica12Copy
  ;;

  vertica11)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11DataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica11Copy
  ;;
    
  vertica10)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10ShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10DataTypes
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10TestSuite
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
  ;;

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
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh ydbCopy
  ;;

  cdb)
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh cdbShortRegression
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh cdbDataTypes
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh cdbTestSuite
  ;;

  copy) 
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh stageCSVDataSets	
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh oracleCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh postgresCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mysqlCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mariadbCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh verticaCopy
	source $YADAMU_SCRIPT_DIR/runRegressionTest.sh vertica10Copy
  ;;

  regression)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh $TESTNAME
  ;; 

  custom)
    source $YADAMU_SCRIPT_DIR/runCustomTest.sh $TESTNAME
  ;; 
  
  cmdLine) 
    source  $YADAMU_SCRIPT_DIR/runCmdLineTests.sh
  ;;

  service)
    source $YADAMU_SCRIPT_DIR/runServiceTests.sh $TESTNAME
  ;; 
  

  interactive)
    sleep 365d
  ;; 
  
  
  *)
    echo "Invalid Test $YADAMU_TEST_NAME: Valid values are shortRegression, export, snowflake, sqlite, vertica, vertica09, cmdLine, copy, upload, interactive or custom"
  ;;
esac