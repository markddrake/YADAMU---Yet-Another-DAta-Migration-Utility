
import {
  Readable,
  PassThrough
}                                     from 'stream'
import {
  pipeline
}                                     from 'stream/promises'

/*
**
** A partition level copy stages a table as a list of files, one per partition. When the table is read as a single task (no partition level reads),
** it is read as one stream that delivers the content of each file in turn.
**
** Each file is complete in its own right: it has its own IV when encrypted and its own deflate/gzip stream when compressed, so each file is decoded
** on its own before its content is appended. A CSV file is a run of rows, so CSV content can be appended as is. A JSON file is one array of rows,
** so the arrays are merged into a single array, which is what the JSON parser expects to find in a data file.
**
*/

const OPEN_ARRAY   = 0x5B // [
const CLOSE_ARRAY  = 0x5D // ]
const NEWLINE      = 0x0A // \n

const isWhitespace = (c) => { return c === 0x20 || c === 0x0A || c === 0x0D || c === 0x09 }

class CSVContent {

  begin() { return [] }

  beginFile() {
	this.lastByte = undefined
  }

  append(chunk) {
	this.lastByte = chunk[chunk.length-1]
	return [chunk]
  }

  endFile(filename) {
	// Make sure the last row of a file is terminated before the first row of the next file
	return ((this.lastByte === undefined) || (this.lastByte === NEWLINE)) ? [] : [Buffer.from('\n')]
  }

  end() { return [] }

}

class JSONContent {

  /*
  **
  ** Drop the opening '[' and the closing ']' of each file and emit the rows between them, separated by a ',' from the rows of the previous files.
  ** Trailing ']' and whitespace are held back until more content arrives, since only the last ']' in the file closes the array.
  **
  */

  begin() {
	this.rowsEmitted = false
	return [Buffer.from('[')]
  }

  beginFile() {
	this.opened = false
	this.fileHasRows = false
	this.held = Buffer.alloc(0)
  }

  emitRows(content) {
	const output = []
	if (!this.fileHasRows) {
	  let start = 0
	  while ((start < content.length) && isWhitespace(content[start])) start++
	  content = content.subarray(start)
	  if (content.length === 0) {
		return output
	  }
	  this.fileHasRows = true
	  if (this.rowsEmitted) {
		output.push(Buffer.from(','))
	  }
	  this.rowsEmitted = true
	}
	output.push(content)
	return output
  }

  append(chunk) {
	let start = 0
	if (!this.opened) {
	  while ((start < chunk.length) && isWhitespace(chunk[start])) start++
	  if (start === chunk.length) {
		return []
	  }
	  if (chunk[start] !== OPEN_ARRAY) {
		throw new Error(`Unexpected character '${String.fromCharCode(chunk[start])}' before the start of the data array.`)
	  }
	  this.opened = true
	  start++
	}
	const content = this.held.length === 0 ? chunk.subarray(start) : Buffer.concat([this.held,chunk.subarray(start)])
	let tail = content.length
	while ((tail > 0) && ((content[tail-1] === CLOSE_ARRAY) || isWhitespace(content[tail-1]))) tail--
	this.held = content.subarray(tail)
	return tail > 0 ? this.emitRows(content.subarray(0,tail)) : []
  }

  endFile(filename) {
	if (!this.opened) {
	  // Empty file
	  return []
	}
	const close = this.held.lastIndexOf(CLOSE_ARRAY)
	if (close < 0) {
	  throw new Error(`Data array not terminated in "${filename}".`)
	}
	return this.emitRows(this.held.subarray(0,close))
  }

  end() {
	return [Buffer.from(']')]
  }

}

async function* readDataFiles(dbi,filenames,content) {

  yield* content.begin()
  for (const filename of filenames) {
	content.beginFile()
	const inputStream = await dbi._getInputStream(filename)
	const decoders = await dbi.getDecoders(filename)
	const decoded = new PassThrough()
	const decoding = pipeline(inputStream,...decoders,decoded)
	// Errors are raised to the consumer by the for await loop. Make sure an abandoned pipeline does not become an unhandled rejection.
	decoding.catch(() => {})
	for await (const chunk of decoded) {
	  if (chunk.length > 0) {
		yield* content.append(chunk)
	  }
	}
	await decoding
	yield* content.endFile(filename)
  }
  yield* content.end()

}

class DataFileSequence {

  static createReadStream(dbi,filenames,contentType) {
	const content = contentType === 'JSON' ? new JSONContent() : new CSVContent()
	return Readable.from(readDataFiles(dbi,filenames,content),{objectMode: false})
  }

}

export { DataFileSequence as default }
