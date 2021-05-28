export BASE_FOLDER=$1
if [ -e $BASE_FOLDER ]; then rm -rf $BASE_FOLDER; fi
mkdir -p $BASE_FOLDER;
