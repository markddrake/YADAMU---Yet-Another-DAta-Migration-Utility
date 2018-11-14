const path = require('path')
const sourceFile = process.argv[2];
console.log(sourceFile)
let src = require(sourceFile);
const fs= require('fs');
const tables = Object.keys(src.metadata);
tables.forEach(function(table) {
                const array = src.data[table];
                console.log(table,array.length);
                array.sort(function (a,b){
                if (a[0] < b[0]) return -1
                if (a[0] > b[0]) return 1;
                if (a[1] < b[1]) return -1;
                if (a[1] > b[1]) return 1;
                return(0)
              })
})
const filename = path.basename(process.argv[2]);
fs.writeFileSync('Wip/' + filename,JSON.stringify(src,' ',2));
