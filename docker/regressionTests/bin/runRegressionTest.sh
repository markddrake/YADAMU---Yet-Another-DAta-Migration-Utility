# Run from YADAMU_HOME
# YADAMU_SKIP_TESTS (space separated test names) skips those tests, for example when the orchestrator couldn't start the
# yadamu-db3 database they need (docker/orchestrator/ensureDb3.sh). This file is sourced, so return to the caller.
if [[ " ${YADAMU_SKIP_TESTS:-} " == *" $1 "* ]]; then
  echo "$(date -u +%Y-%m-%dT%H:%M:%S.%3NZ) [WARNING][QA][REGRESSION][$1]: Skipped (YADAMU_SKIP_TESTS)."
  return 0 2>/dev/null || exit 0
fi
export YADAMU_TASK=$1
unset YADAMU_LOG_PATH
export YADAMU_LOG_ROOT=$YADAMU_HOME/log
source $YADAMU_QA_HOME/bin/initializeLogging.sh $YADAMU_TEST
if [ -z ${NODE_NO_WARNINGS+x} ]; then export NODE_NO_WARNINGS=1; fi
if [ -z ${NODE_OPTIONS+x} ]; then export NODE_OPTIONS="--max_old_space_size=8192"; fi
if [ -e $YADAMU_HOME/log/$YADAMU_TASK.log ]; then rm $YADAMU_HOME/log/$YADAMU_TASK.log; fi
touch  $YADAMU_LOG_PATH/$YADAMU_TASK.log
#ln -s $YADAMU_LOG_PATH/$YADAMU_TASK.log $YADAMU_HOME/log/$YADAMU_TASK.log'docker 
npm ls > $YADAMU_LOG_PATH/$YADAMU_TASK.env
# The machine that produced the results, and the Oracle Instant Client (npm ls doesn't show it)
{
  echo "# Hardware"
  node -e '
    const os = require("os"), fs = require("fs")
    const read = (f) => { try { return fs.readFileSync(f, "utf8").trim() } catch (e) { return "unknown" } }
    const gb = (b) => (b / 1073741824).toFixed(1) + " GB"
    const cpus = os.cpus()
    const limit = process.constrainedMemory ? process.constrainedMemory() : 0
    console.log("CPU:", cpus.length ? cpus[0].model : "unknown", "x", cpus.length, "threads")
    console.log("Machine:", read("/sys/class/dmi/id/sys_vendor"), read("/sys/class/dmi/id/product_name"), "/", read("/sys/class/dmi/id/board_name"))
    console.log("RAM:", gb(os.totalmem()), "host,", (limit > 0 && limit < os.totalmem()) ? gb(limit) : "no", "container limit")
    let disks = []
    try { disks = fs.readdirSync("/sys/block").filter((d) => !/^(loop|ram|dm-|zram|sr|md)/.test(d)) } catch (e) {}
    for (const d of disks) console.log("Disk:", d, read(`/sys/block/${d}/device/model`), read(`/sys/block/${d}/queue/rotational`) === "0" ? "SSD" : "rotational", gb(Number(read(`/sys/block/${d}/size`)) * 512))
    console.log("Kernel:", os.release(), "Node:", process.version)
  '
  echo "# Oracle Instant Client"
  dpkg-query -W -f='${Package} ${Version}\n' 'oracle-instantclient*' 2>/dev/null || echo "not installed"
} >> $YADAMU_LOG_PATH/$YADAMU_TASK.env
node $YADAMU_HOME/src/qa/cli/test.js CONFIG=$YADAMU_QA_HOME/regression/$YADAMU_TASK.json EXCEPTION_FOLDER=$YADAMU_LOG_PATH 2>&1 | tee $YADAMU_LOG_PATH/$YADAMU_TASK.log 