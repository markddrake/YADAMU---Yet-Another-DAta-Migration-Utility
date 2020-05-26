export YADAMU_HOME=`pwd`
export YADAMU_QA_HOME=$YADAMU_HOME/qa
export YADAMU_SCRIPT_DIR=$(dirname "${BASH_SOURCE[0]}")
source qa/bin/createOutputFolders.sh mnt
export YADAMU_TEST_NAME=${YADAMU_TEST_NAME:-all}
case $YADAMU_TEST_NAME  in
  export)
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

  all)
    source $YADAMU_SCRIPT_DIR/runRegressionTests.sh 
  ;;	
  *)
    echo "valid values are export, import, fileRoundtrip, dbRoundtrip, or all (default)"
  ;;
esac