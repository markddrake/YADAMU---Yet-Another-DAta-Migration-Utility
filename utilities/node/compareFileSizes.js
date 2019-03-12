"use strict";
const fs = require('fs').promises
const f = require('fs');
const path = require('path')
const assert = require('assert')

const logFile = f.createWriteStream(process.argv[2] + path.sep + 'fileSizes.log',{flags: "a"});
const sourceDir = process.argv[3];
const targetDir = process.argv[4];

function compareFiles(sourceFile,targetFile) {
   
console.log(`${new Date().toISOString()}: Comparing "${sourceFile}" and "${targetFile}".`);
  assert(f.existsSync(sourceFile),'Source File Not Found');  
  assert(f.existsSync(targetFile),'Source File Not Found');
  const source = require(path.resolve(sourceFile));
  const target = require(path.resolve(targetFile));
}


function printFileInfo(files) {
    
    const regExp =  new RegExp("\B(?=(\d{3})+(?!\d))","g");
    for (const fidx in files) {
      files[fidx] = Object.assign(files[fidx], f.statSync(files[fidx].path))
    }
    
    const drift1 = files[0].size - files[1].size;
    const drift2 = files[1].size - files[2].size;
    
    logFile.write(`| ${files[0].name.padEnd(32)} |`
                + ` ${drift1 === 0 ? 'SUCCESSFUL'.padStart(12) : 'FAILED'.padStart(12)} |`
                + ` ${drift1.toString().replace(regExp, ",").padStart(12)} |`
                + ` ${drift2 === 0 ? 'SUCCESSFUL'.padStart(12) : 'FAILED'.padStart(12)} |`
                + ` ${drift2.toString().replace(regExp, ",").padStart(12)} |`
                + ` ${files[0].size.toString().replace(regExp, ",").padStart(12)} |`
                + ` ${files[1].size.toString().replace(regExp, ",").padStart(12)} |`
                + ` ${files[2].size.toString().replace(regExp, ",").padStart(12)} |\n`)

}
     
function compareFolders(sourceDir,targetDir,filenames,targetSuffixes) {
   
    logFile.write('+' + '-'.repeat(32+12+12+12+12+12+12+12+25-2) + '+' + '\n') 
  
    logFile.write(`| ${'SOURCE FILE'.padEnd(32)} |`
                + ` ${'LOAD'.padStart(12)} |`
                + ` ${'DELTA'.padStart(12)} |`
                + ` ${'CLONE'.padStart(12)} |`
                + ` ${'DELTA'.padStart(12)} |`
                + ` ${'SRC SIZE'.padStart(12)} |`
                + ` ${'SIZE #1'.padStart(12)} |`
                + ` ${'SIZE #2'.padStart(12)} |\n`)
               
    logFile.write('+' + '-'.repeat(32+12+12+12+12+12+12+12+25-2) + '+' + '\n') 

    for (const filename in filenames) {
     if (filenames[filename].endsWith('.json')) {
       const files = []
       files.push({ name : filenames[filename], path : path.resolve(sourceDir + path.sep + filenames[filename])});
       for (const s in targetSuffixes) {
         const sourceFilename = filenames[filename].slice(0,-5) + targetSuffixes[s] + ".json"
         files.push({ name: sourceFilename, path : path.resolve(targetDir + path.sep + sourceFilename)});
       }
       printFileInfo(files);
     };
   }
    
   logFile.write('+' + '-'.repeat(32+12+12+12+12+12+12+12+25-2) + '+' + '\n\n') 
}
  
async function doCompare(sourceDir,targetDir) {

  const filenames = await fs.readdir(sourceDir);
  await compareFolders(sourceDir,targetDir,filenames,['1','2']);

}

doCompare(sourceDir,targetDir);

 