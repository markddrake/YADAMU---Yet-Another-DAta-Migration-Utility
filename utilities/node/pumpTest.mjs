import Pump from '../../src/node/cli/pump.js'

async function main() {
  const pump = new Pump();
  const results = await pump.pump(process.env.PUMP_JOB)
  console.log(results)
}

main().then(() => {console.log('Success')}).catch((e) => {console.log(e)})