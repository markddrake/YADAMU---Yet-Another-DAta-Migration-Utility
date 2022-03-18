"use strict";
import fs          from 'fs/promises'
import f           from 'fs'
import path        from 'path'
import assert      from 'assert'
import crypto      from 'crypto'

const logFile = f.createWriteStream(process.argv[2] + path.sep + 'arrayContent.log',{flags: "a"});
const sourceDir = process.argv[3];
const targetDir = process.argv[4];


const deepCompare = ( process.argv[5] === 'true' );
const sortArrays = ( process.argv[6] === 'true' );

function vendorUsesLowerCaseIdentifiers(serverVendor) {
serverVendor
  if ((serverVendor !== undefined)  && ((serverVendor.indexOf('mariadb') > -1) || (serverVendor.indexOf('MySQL') > -1))) {
    return true;
  }
  return false;
}

function sortRows(array) {
    
  array.sort(function (a,b){
     for (const i in array) {
       if (a[i] < b[i]) return -1
       if (a[i] > b[i]) return 1;
     }
  })
  
  return array
}

function hashContent(content) {
 
  if (this.sortArrays) {
    return crypto.createHash('sha256').update(JSON.stringify(sortRows(content))).digest('hex');
    }
  else {
    return crypto.createHash('sha256').update(JSON.stringify(content)).digest('hex');
  }
}

function getTableMetadata(tableInfo,content) {
  
  tableInfo.rowCount = content.length;
  tableInfo.byteCount =  JSON.stringify(content).length;
  if (deepCompare) {
    tableInfo.hashValues = hashContent(content);
  }
  
}

function processContent(tables,content) {
    
  const tryLowerCaseIdentifiers = vendorUsesLowerCaseIdentifiers(content.systemInformation.serverVendor);
  const fileMetadata = {}
  
  // Do not use forEach or map do invoke getTableMetadata to avoid running out of heap.
  
  for (const table of tables) {
    const tableMetadata = {
      rowCount  : -1
     ,byteCount : -1
     ,hashValue : "0x0"
    }     
    if (content.data !== undefined) {
      if (content.data[table] !== undefined) {
        getTableMetadata(tableMetadata,content.data[table])
      }
      else {
        if ((tryLowerCaseIdentifiers) && (content.data[table.toLowerCase()] !== undefined)) {
          getTableMetadata(tableMetadata,content.data[table.toLowerCase()])
        }
      }
    }
    fileMetadata[table] = tableMetadata
  }
   
  return fileMetadata

}

function generateMissingMetadata(tables) {

  const fileMetadata = {}
  
  // Do not use forEach or map do invoke getTableMetadata to avoid running out of heap.
  
  for (const table of tables) {
    const tableMetadata = {
      rowCount  : -1
     ,byteCount : -1
     ,hashValue : "0x0"
    }     
    fileMetadata[table] = tableMetadata
  }
   
  return fileMetadata

}

function compareArrayMetadata(files){
          
  const regExp =  new RegExp("\B(?=(\d{3})+(?!\d))","g")

  let content = JSON.parse(f.readFileSync(files[0].path))
  const tables = Object.keys(content.metadata).sort()
                          
  files[0].metadata = processContent(tables,content);

  try {
    content = JSON.parse(f.readFileSync(files[1].path))
    files[1].metadata = processContent(tables,content)
  } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') {
      throw e;
    }
    files[1].metadata =  generateMissingMetadata(tables)
  }
  
  try {
    content = JSON.parse(f.readFileSync(files[2].path))
    files[2].metadata = processContent(tables,content)
  } catch (e) {
    if (e.code !== 'MODULE_NOT_FOUND') {
      throw e;
    }
    files[2].metadata = generateMissingMetadata(tables)
  }
  
  logFile.write('+' + '-'.repeat(48+10+10+10+12+10+10+10+12+34-2) + '+' + '\n') 
  logFile.write(`| ${files[0].name.padStart(48)} |`
              + ` ${'SRC ROWS'.padStart(10)} |`
              + ` ${'ROWS #1'.padStart(11)} |`
              + ` ${'ROWS #2'.padStart(11)} |` 
              + ` ${'RESULTS'.padStart(13)} |`
              + ` ${'SRC BYTES'.padStart(11)} |`
              + ` ${'BYTES #1'.padStart(11)} |`
              + ` ${'BYTES #2'.padStart(11)} |` 
              + ` ${'RESULTS'.padStart(12)} |\n`);
  logFile.write('+' + '-'.repeat(48+10+10+10+12+10+10+10+12+34-2) + '+' + '\n');
  tables.forEach(function(table) {  
               logFile.write(`| ${table.padStart(48)} |`
                            + ` ${files[0].metadata[table].rowCount.toString().replace(regExp, ",").padStart(10)} | `
                            + ` ${files[1].metadata[table].rowCount.toString().replace(regExp, ",").padStart(10)} | `
                            + ` ${files[2].metadata[table].rowCount.toString().replace(regExp, ",").padStart(10)} | ` 
                            + ` ${((files[0].metadata[table].rowCount === files[1].metadata[table].rowCount) && (files[1].metadata[table].rowCount=== files[2].metadata[table].rowCount)) ? 'MATCH'.padStart(12) : ' MISMATCH'.padStart(12)} | `
                            + ` ${files[0].metadata[table].byteCount.toString().replace(regExp, ",").padStart(10)} | `
                            + ` ${files[1].metadata[table].byteCount.toString().replace(regExp, ",").padStart(10)} | `
                            + ` ${files[2].metadata[table].byteCount.toString().replace(regExp, ",").padStart(10)} |` 
                            + ` ${((files[0].metadata[table].byteCount === files[1].metadata[table].byteCount) && (files[1].metadata[table].byteCount === files[2].metadata[table].byteCount)) ? 'MATCH'.padStart(12) : 'MISMATCH'.padStart(12)} |`);
                if (deepCompare) {
                  logFile.write(` ${files[0].metadata[table].hashValue.padStart(25)} | `
                              + ` ${files[1].metadata[table].hashValue.padStart(25)} | `
                              + ` ${files[2].metadata[table].hashValue.padStart(25)} |` 
                              + ` ${((files[0].metadata.hashValue === files[1].metadata[table].hashValue) && (files[1].metadata[table].hashValue === files[2].metadata[table].hashValue)) ? 'MATCH'.padStart(12) : ' MISMATCH'.padStart(12)} |`)
                }
                logFile.write(`\n`)                            
  })
  logFile.write('+' + '-'.repeat(48+10+10+10+12+10+10+10+12+34-2) + '+' + '\n');
  logFile.write(`\n`)
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
