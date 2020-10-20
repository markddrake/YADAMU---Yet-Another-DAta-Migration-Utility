export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
export YADAMU_TEST_NAME=${YADAMU_TEST_NAME:-all}
case $YADAMU_TEST_NAME  in
  export)
    source qa/bin/createOutputFolders.sh mnt
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh export
  ;;
  
  import)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh import
  ;;
  
  fileRoundtrip)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh fileRoundtrip
	;;
	
  dbRoundtrip)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh dbRoundtrip
  ;;
    
  lostConnection)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh lostConnection
  ;;

  mongoImport)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoImport
  ;;

  mongoRoundtrip)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh mongoRoundtrip
  ;;

  all)
    source $YADAMU_SCRIPT_DIR/runRegressionTests.sh 
  ;;	
  
  cmdLine) 
    source  $YADAMU_SCRIPT_DIR/runCmdTests.sh
  ;;

  interactive)
    sleep 365d
  ;; 
  
  snowflake)
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeImport
    source $YADAMU_SCRIPT_DIR/runRegressionTest.sh snowflakeRoundtrip
  ;;  
  
  *)
    echo "Invalid Test $YADAMU_TEST_NAME: Valid values are export, import, fileRoundtrip, dbRoundtrip, lostConnection,  mongoImport, mongoRoundtrip, snowflake, cmdLine, interactive or all (default)"
  ;;
esac