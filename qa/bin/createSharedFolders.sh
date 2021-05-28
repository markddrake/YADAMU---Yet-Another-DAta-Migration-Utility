export YADMAU_OUTPUT_FOLDER=$1
if [ ! -e $YADMAU_OUTPUT_FOLDER ]; then mkdir $YADMAU_OUTPUT_FOLDER; fi
mkdir -p $YADMAU_OUTPUT_FOLDER/log
mkdir -p $YADMAU_OUTPUT_FOLDER/longRegress
mkdir -p $YADMAU_OUTPUT_FOLDER/shortRegress
mkdir -p $YADMAU_OUTPUT_FOLDER/stagingArea
mkdir -p $YADMAU_OUTPUT_FOLDER/cmdLine
mkdir -p $YADMAU_OUTPUT_FOLDER/output
mkdir -p $YADMAU_OUTPUT_FOLDER/scratch
mkdir -p $YADMAU_OUTPUT_FOLDER/test
mkdir -p $YADMAU_OUTPUT_FOLDER/work
