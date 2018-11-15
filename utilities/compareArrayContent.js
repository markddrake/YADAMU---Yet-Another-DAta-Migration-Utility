"use strict";
const fs = require('fs').promises
const f = require('fs');
const path = require('path')
const assert = require('assert')
const crypto = require('crypto');

const logFile = f.createWriteStream(process.argv[2] + path.sep + 'arrayContent.log',{flags: "a"});
const sourceDir = process.argv[3];
const targetDir = process.argv[4];


const contentCompare = ( process.argv[5] === 'true' );
const sortedCompare = ( process.argv[6] === 'true' );

function sortRows(array) {
    
  array.sort(function (a,b){
     for (const i in array) {
       if (a[i] < b[i]) return -1
       if (a[i] > b[i]) return 1;
     }
  })
  
  return array
}

function deepCompare(content,tableInfo) {
  if (sortedCompare) {
    tableInfo.hashes.unshift(crypto.createHash('sha256').update(JSON.stringify(sortRows(content))).digest('hex'));
  }
  else {
    tableInfo.hashes.unshift(crypto.createHash('sha256').update(JSON.stringify(content)).digest('hex'));
  }
}
  
function shallowCompare(content,tableInfo) {
  if (content === undefined) {
    tableInfo.rowCounts.push(-1);
    tableInfo.byteCounts.push(-1)
  }
  else {
    tableInfo.rowCounts.push(content.length)
    tableInfo.byteCounts.push(JSON.stringify(content).length)
  }
}

function compareFiles(files){
          
  const regExp =  new RegExp("\B(?=(\d{3})+(?!\d))","g");

  const tableInfo = {}  
  let content = require(files[0].path);
  const tables = Object.keys(content.metadata).sort();

  tables.forEach(function(table) {
            tableInfo[table] = { 
              tableName : table
             ,rowCounts : []
             ,byteCounts : []
            }
  })
              
  tables.forEach(function(table) {
     shallowCompare(content.data[table],tableInfo[table]);
  })

  content = require(files[1].path);
  tables.forEach(function(table) {
     shallowCompare(content.data[table],tableInfo[table]);
  })
  
  content = require(files[2].path);
  tables.forEach(function(table) {
     shallowCompare(content.data[table],tableInfo[table]);
  })

  if (contentCompare) {   
    tables.forEach(function(table) {
       tableInfo.hashes = []
       deepCompare(content.data[table],tableInfo[table]);
    })
    content = require(files[1].path);
    tables.forEach(function(table) {
       deepCompare(content.data[table],tableInfo[table]);
    })
    content = require(files[0].path);
    tables.forEach(function(table) {
       deepCompare(content.data[table],tableInfo[table]);
    })
  }
 
  logFile.write(`${files[0].name}:\n`)
  tables.forEach(function(table) {  
               const info = tableInfo[table]
               logFile.write(`${info.tableName.padStart(40)} Rows: [`
                            + `${info.rowCounts[0].toString().replace(regExp, ",").padStart(10)},`
                            + `${info.rowCounts[1].toString().replace(regExp, ",").padStart(10)},`
                            + `${info.rowCounts[2].toString().replace(regExp, ",").padStart(10)}].` 
                            + `${((info.rowCounts[0] === info.rowCounts[1]) && (info.rowCounts[1] === info.rowCounts[2])) ? ' Success'.padEnd(16) : ' Mismatch'.padEnd(16)} Byte Counts: [`
                            + `${info.byteCounts[0].toString().replace(regExp, ",").padStart(10)},`
                            + `${info.byteCounts[1].toString().replace(regExp, ",").padStart(10)},`
                            + `${info.byteCounts[2].toString().replace(regExp, ",").padStart(10)}].` 
                            + `${((info.byteCounts[0] === info.byteCounts[1]) && (info.byteCounts[1] === info.byteCounts[2])) ? ' Success'.padEnd(16) : ' Mismatch'.padEnd(16)} `
                            + `${(info.hashes) ? 'Hashes: [' + info.hashes[0] + ',' + info.hashes[1] +',' + info.hashes[2] + '].' : ''}`
                            + `\n`)
                             
  })
}

     
function compareFolders(sourceDir,targetDir,filenames,targetSuffixes) {
   
   for (const filename in filenames) {
     if (filenames[filename].endsWith('.json')) {
       const files = []
       files.push({ name : filenames[filename], path : path.resolve(sourceDir + path.sep + filenames[filename])});
       for (const s in targetSuffixes) {
         const sourceFilename = filenames[filename].slice(0,-5) + targetSuffixes[s] + ".json"
         files.push({ name: sourceFilename, path : path.resolve(targetDir + path.sep + sourceFilename)});
       }
       compareFiles(files);
     };
   }
    
}
  
async function doCompare(sourceDir,targetDir) {

  const filenames = await fs.readdir(sourceDir);
  await compareFolders(sourceDir,targetDir,filenames,['1','2']);

}

doCompare(sourceDir,targetDir);
