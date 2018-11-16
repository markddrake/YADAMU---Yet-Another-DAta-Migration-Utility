"use strict";
const fs = require('fs').promises
const f = require('fs');
const path = require('path')
const assert = require('assert')
const crypto = require('crypto');

const logFile = f.createWriteStream(process.argv[2] + path.sep + 'arrayContent.log',{flags: "a"});
const sourceDir = process.argv[3];
const targetDir = process.argv[4];


const compareArrayContent = ( process.argv[5] === 'true' );
const sortArrays = ( process.argv[6] === 'true' );

function sortRows(array) {
    
  array.sort(function (a,b){
     for (const i in array) {
       if (a[i] < b[i]) return -1
       if (a[i] > b[i]) return 1;
     }
  })
  
  return array
}

function deepCompare(tableInfo,content) {
 
  if (sortArrays) {
    tableInfo.hashes.unshift(crypto.createHash('sha256').update(JSON.stringify(sortRows(content))).digest('hex'));
    }
  else {
    tableInfo.hashes.unshift(crypto.createHash('sha256').update(JSON.stringify(content)).digest('hex'));
  }
}

function shallowCompare(content,tableInfo) {

  tableInfo.rowCounts.push(content.length)
  tableInfo.byteCounts.push(JSON.stringify(content).length)

}

function checkVendor(systemInformation) {
    
  if (systemInformation.serverVendor && ((systemInformation.serverVendor.indexOf('mariadb') > -1) || (systemInformation.serverVendor.indexOf('MySQL') > -1))) {
    return true;
  }
  return false;
}

function processArrayContent(tableName,tableInfo,content) {

   // Avoid content comapare if rows count or byte count do not match
   
  const tableData = tableInfo[tableName];
  
  if ((tableData.rowCounts[0] === tableData.rowCounts[1]) && (tableData.rowCounts[0] === tableData.rowCounts[2])) {
    if ((tableData.byteCounts[0] === tableData.byteCounts[1]) && (tableData.byteCounts[0] === tableData.byteCounts[2])) {
      const tryLowerCaseName = checkVendor(content.systemInformation);
      let arrayContent = content.data[tableName]
      if (arrayContent) {
        deepCompare(tableData,arrayContent);
      }
      else {
        if (tryLowerCaseName) {
          arrayContent = content.data[tableName.toLowerCase()]
          deepCompare(tableData,arrayContent);
        }
      }
    }
  }
}
   
function processArrayMetadata(tableName,tableInfo,content) {

  const tryLowerCaseName = checkVendor(content.systemInformation);
  let arrayContent = content.data[tableName]
  const tableData = tableInfo[tableName];
  
  if (arrayContent) {
    shallowCompare(arrayContent,tableData);
  }
  else {
    if (tryLowerCaseName) {
      arrayContent = content.data[tableName.toLowerCase()]
      shallowCompare(arrayContent,tableData);
    }
    else {
      tableData.rowCounts.push(-1);
      tableData.byteCounts.push(-1)
    }           
  }
  
}

function compareArrayMetadata(files){
          
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
     processArrayMetadata(table,tableInfo,content);
  })

  content = require(files[1].path);
  tables.forEach(function(table) {
     processArrayMetadata(table,tableInfo,content);
  })
  
  content = require(files[2].path);
  tables.forEach(function(table) {
     processArrayMetadata(table,tableInfo,content);
  })
  
  if (compareArrayContent) {
    // Process tables in reverse order. - Saves reading the third file twice.
  
    tables.forEach(function(table) {
       tableInfo[table].hashes = []
       processArrayContent(table,tableInfo,content);
    })
    content = require(files[1].path);
    tables.forEach(function(table) {
       processArrayContent(table,tableInfo,content);
    })
    content = require(files[0].path);
    tables.forEach(function(table) {
       processArrayContent(table,tableInfo,content);
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
                            + `${((info.byteCounts[0] === info.byteCounts[1]) && (info.byteCounts[1] === info.byteCounts[2])) ? ' Success'.padEnd(16) : ' Mismatch'.padEnd(16)} `);
                if (info.hashes && (info.hashes.length > 2)) {
                  logFile.write(`Hashes : ${((info.hashes[0] === info.hashes[1]) && (info.hashes[1] === info.hashes[2])) ? ' Success'.padEnd(16) : ' Mismatch'.padEnd(16)}`)
                }
                logFile.write(`\n`)
                             
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
       compareArrayMetadata(files);
     };
   }
    
}
  
async function doCompare(sourceDir,targetDir) {

  const filenames = await fs.readdir(sourceDir);
  await compareFolders(sourceDir,targetDir,filenames,['1','2']);

}

doCompare(sourceDir,targetDir);
