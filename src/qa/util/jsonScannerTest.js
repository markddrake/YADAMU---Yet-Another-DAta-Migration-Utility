/*
**
** Regression test for JSONScanner and the parsers built on it (file/jsonParser.js, loader/jsonParser.js, loader/rowCounter.js).
**
** Every parsed row is checked against JSON.parse() of the same text, for each layout (compact, pretty printed, CRLF ...), for chunk sizes
** from 1 byte up, and with LARGE_VALUE_THRESHOLD lowered so that every row is split into separately parsed members.
**
** node src/qa/util/jsonScannerTest.js
** BIG=1 node --max-old-space-size=8192 src/qa/util/jsonScannerTest.js    also parses a single row with two 300MB columns
**
*/

import assert                         from 'assert'

import {
  Readable,
  Writable
}                                     from 'stream'

import {
  pipeline
}                                     from 'stream/promises'

import JSONScanner                    from '../../node/dbi/file/jsonScanner.js'
import FileParser                     from '../../node/dbi/file/jsonParser.js'
import LoaderParser                   from '../../node/dbi/loader/jsonParser.js'
import RowCounter                     from '../../node/dbi/loader/rowCounter.js'

import {
  IncompleteJSON,
  InvalidJSON
}                                     from '../../node/dbi/file/fileException.js'

const LOGGER = { handleException() {}, info() {}, trace() {}, error() {} }

let checks = 0

function chunk(buffer,maxChunkSize) {
  const chunks = []
  for (let i = 0; i < buffer.length;) {
    const size = maxChunkSize === 1 ? 1 : 1 + Math.floor(Math.random() * maxChunkSize)
    chunks.push(buffer.subarray(i,i + size))
	i += size
  }
  return chunks
}

async function parse(Parser,mode,buffer,maxChunkSize) {
  const messages = []
  const parser = new Parser(mode,'test',{},LOGGER)
  try {
    await pipeline(Readable.from(chunk(buffer,maxChunkSize)),parser,new Writable({objectMode: true, write(message,encoding,callback) { messages.push(message.eod ? {eod: {parsed: message.eod.parsed}} : message); callback() }}))
  } catch (e) {
	return {messages, error: e}
  }
  return {messages}
}

async function countRows(buffer,maxChunkSize) {
  const counter = new RowCounter('test',LOGGER)
  await pipeline(Readable.from(chunk(buffer,maxChunkSize)),counter,new Writable({write(data,encoding,callback) { callback() }}))
  return counter.ROW_COUNT
}

const ROWS = [
  ["173887","document","quotes \" brackets ] [ } { , : and a backslash \\",null,16,"line\nbreak\ttab é 日本 😀 \u2028 \u0000",[-0.0087,1e-7,-0,3.5,12345678901234567890],{"ext":".pdf","nested":{"a":[1,2,{"b":"}"}]},"":0},"2026-06-17T22:01:59.460324Z",true,false]
, ["2","x","",null,0,"",[],{},null,"\\","\\\""]
, [Array.from({length: 1024},(v,i) => Math.sin(i) / 3),"ab".repeat(5000),{"deep":[[[[["x"]]]]]}]
, [{"__proto__":{"polluted":1},"k":"v"}]
]

const EXPORT_FILE = {
  systemInformation : {date: "2026-10-08", vendor: "Postgres", nested: {a: [1,{b: "]"}]}}
, metadata          : {"T1": {columnNames: ["a"]}, "T \"2\"": {columnNames: ["b"]}, "EMPTY": {columnNames: []}}
, ddl               : ["create table \"T1\" (a int)","create table \"T \\\"2\\\"\" (b text)"]
, data              : {"T1": ROWS, "T \"2\"": ROWS.slice(0,2), "EMPTY": []}
}

const LAYOUTS = {
  compact     : (v) => JSON.stringify(v)
, pretty      : (v) => JSON.stringify(v,null,2)
, tabs        : (v) => JSON.stringify(v,null,'\t')
, crlf        : (v) => JSON.stringify(v,null,2).replace(/\n/g,'\r\n')
, padded      : (v) => `\n\n  ${JSON.stringify(v)}  \r\n\n`
}

function expectedExportMessages(mode) {
  const messages = []
  for (const key of ['systemInformation','metadata','ddl']) {
	if ((mode !== 'DATA_ONLY') || (key !== 'ddl')) {
	  messages.push({[key]: EXPORT_FILE[key]})
	}
  }
  for (const [table,rows] of Object.entries(EXPORT_FILE.data)) {
	messages.push({table: table})
	rows.forEach((row) => { messages.push({data: row}) })
	messages.push({eod: {parsed: rows.length}})
  }
  messages.push({eof: true})
  return messages
}

async function testWellFormed() {
  const expectedRows = JSON.parse(JSON.stringify(ROWS))
  for (const threshold of [undefined,64]) {
    JSONScanner.LARGE_VALUE_THRESHOLD = threshold
    for (const maxChunkSize of [1,3,97,65536,1 << 24]) {
      for (const [layout,format] of Object.entries(LAYOUTS)) {
		const context = `${layout}, chunks <= ${maxChunkSize}, threshold ${threshold ?? 'default'}`

		const loaderFile = Buffer.from(format(ROWS))
        const loader = await parse(LoaderParser,'DATA_ONLY',loaderFile,maxChunkSize)
		assert.ifError(loader.error)
        assert.deepStrictEqual(loader.messages.map((message) => message.data),expectedRows,`Loader rows: ${context}`)
		assert.strictEqual(await countRows(loaderFile,maxChunkSize),ROWS.length,`Row count: ${context}`)

		const exportFile = Buffer.from(format(EXPORT_FILE))
		for (const mode of ['DDL_AND_DATA','DATA_ONLY','DDL_ONLY']) {
          const result = await parse(FileParser,mode,exportFile,maxChunkSize)
		  assert.ifError(result.error)
          assert.deepStrictEqual(result.messages,JSON.parse(JSON.stringify(expectedExportMessages(mode))),`Export file ${mode}: ${context}`)
		}
		checks += 5
	  }
	}
  }
  JSONScanner.LARGE_VALUE_THRESHOLD = undefined

  // __proto__ must be an own property, as with JSON.parse(), whether or not the row is split
  for (const threshold of [undefined,8]) {
    JSONScanner.LARGE_VALUE_THRESHOLD = threshold
    const {messages} = await parse(LoaderParser,'DATA_ONLY',Buffer.from('[[{"__proto__":{"polluted":1}}]]'),5)
	assert.strictEqual(Object.getPrototypeOf(messages[0].data[0]),Object.prototype)
	assert.deepStrictEqual(Object.keys(messages[0].data[0]),['__proto__'])
	checks++
  }
  JSONScanner.LARGE_VALUE_THRESHOLD = undefined

  for (const empty of ['[]','[\r\n]\r\n','  [ ]  ']) {
    const {messages,error} = await parse(LoaderParser,'DATA_ONLY',Buffer.from(empty),2)
	assert.ifError(error)
	assert.strictEqual(messages.length,0)
	assert.strictEqual(await countRows(Buffer.from(empty),2),0)
	checks++
  }
}

async function testMalformed() {
  const loaderCases = {
    '[[1,2][3]]'        : InvalidJSON
  , '[[1,2],]'          : InvalidJSON
  , '[[1,2,]]'          : InvalidJSON
  , '[[1,tru]]'         : InvalidJSON
  , '[[1,2}]'           : InvalidJSON
  , '[[1]]]'            : InvalidJSON
  , '[[1]] x'           : InvalidJSON
  , '{"a":[1]}'         : InvalidJSON
  , '\ufeff[[1]]'       : InvalidJSON
  , '[[1,"a\u0000b"]]'  : InvalidJSON
  , '[["abc]]'          : IncompleteJSON
  , '[[1,2],[3'         : IncompleteJSON
  , ''                  : IncompleteJSON
  , '  \n'              : IncompleteJSON
  }
  for (const threshold of [undefined,4]) {
    JSONScanner.LARGE_VALUE_THRESHOLD = threshold
    for (const [text,expected] of Object.entries(loaderCases)) {
      const {error} = await parse(LoaderParser,'DATA_ONLY',Buffer.from(text),3)
      assert.ok(error instanceof expected,`${JSON.stringify(text)}: expected ${expected.name}, received ${error?.constructor.name}: ${error?.message}`)
	  // A SyntaxError would be reported by YadamuOutcome as an invalid request rather than a failed operation
	  assert.ok(!(error instanceof SyntaxError),`${JSON.stringify(text)}: SyntaxError escaped the parser`)
	  checks++
	}
  }
  JSONScanner.LARGE_VALUE_THRESHOLD = undefined

  // UTF-16 must be rejected, not hang
  const utf16 = Buffer.concat([Buffer.from([0xFF,0xFE]),Buffer.from('[[1]]','utf16le')])
  assert.ok((await parse(LoaderParser,'DATA_ONLY',utf16,4)).error instanceof InvalidJSON)

  const exportCases = {
    '{"systemInformation" {}}'                       : InvalidJSON
  , '{systemInformation: {}}'                        : InvalidJSON
  , '{"data": 5}'                                    : InvalidJSON
  , '{"metadata":{"T1":{}},"data":{"T1":[[1]]}'     : IncompleteJSON
  , '[[1]]'                                          : InvalidJSON
  }
  for (const [text,expected] of Object.entries(exportCases)) {
    const {error} = await parse(FileParser,'DDL_AND_DATA',Buffer.from(text),3)
    assert.ok(error instanceof expected,`${JSON.stringify(text)}: expected ${expected.name}, received ${error?.constructor.name}: ${error?.message}`)
	checks++
  }
}

async function testLargeRow() {
  // Two 300MB columns: too large for a single JSON.parse() (V8 strings are limited to 0x1fffffe8 characters), so the row must be split.
  const MB = 1024 * 1024
  const block = Buffer.alloc(MB,'0123456789abcdef')
  async function* file() {
    yield Buffer.from('[[1,"')
	for (let i = 0; i < 300; i++) yield block
	yield Buffer.from('","')
	for (let i = 0; i < 300; i++) yield block
	yield Buffer.from('",{"x":[1,2]}]]')
  }
  let row
  await pipeline(Readable.from(file()),new LoaderParser('DATA_ONLY','test',{},LOGGER),new Writable({objectMode: true, write(message,encoding,callback) { row = message.data; callback() }}))
  assert.strictEqual(row.length,4)
  assert.strictEqual(row[1].length,300 * MB)
  assert.strictEqual(row[2].length,300 * MB)
  assert.deepStrictEqual(row[3],{x: [1,2]})
  checks++
}

await testWellFormed()
await testMalformed()
if (process.env.BIG === '1') {
  await testLargeRow()
}
console.log(`jsonScannerTest: ${checks} checks passed${process.env.BIG === '1' ? ' (including the 600MB row)' : ''}.`)
