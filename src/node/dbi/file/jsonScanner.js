/*
**
** JSONScanner splits a JSON document into the values a YADAMU parser needs, without tokenizing the values it is not interested in.
**
** The scanner walks the outer structure of the document (the containers the handler DESCENDs into) token by token. Every other value
** is located by scanning its bytes, tracking only string, escape and bracket state, and is then either
**
**   CAPTURE: decoded and handed to the native JSON.parse(), or
**   SKIP:    validated structurally and discarded (e.g. when counting rows).
**
** The structural characters are all ASCII, so they can never occur inside a multi-byte UTF-8 sequence, and a value can be cut from the
** Buffer before it is decoded.
**
** JSON.parse() needs the whole value as a single string, and V8 cannot create a string longer than 0x1fffffe8 characters (about 512MB), so a row
** with several large columns could not be parsed as a whole. A CAPTUREd array or object that grows beyond LARGE_VALUE_THRESHOLD bytes is therefore
** split: its bytes are fed to a nested JSONScanner that captures each member separately (splitting any large member in turn) and rebuilds the
** container. Only an individual string or number has to fit in a single JavaScript string.
**
** Handler interface
**
**   decide(depth,key,firstByte)  returns JSONScanner.DESCEND, JSONScanner.CAPTURE or JSONScanner.SKIP for the value that starts at depth (0 is the root).
**                                key is the member name when the value belongs to an object, undefined when it belongs to an array.
**   open(depth,key,isArray)      a DESCENDed container has been opened.
**   close(depth,key,isArray)     a DESCENDed container has been closed.
**   value(depth,key,value)       a CAPTUREd value.
**   skipped(depth,key,firstByte) a SKIPped value.
**
*/

import {
  InvalidJSON
}                from './fileException.js'

const OPEN_ARRAY   = 0x5B // [
const CLOSE_ARRAY  = 0x5D // ]
const OPEN_OBJECT  = 0x7B // {
const CLOSE_OBJECT = 0x7D // }
const QUOTE        = 0x22 // "
const BACKSLASH    = 0x5C // \
const COMMA        = 0x2C // ,
const COLON        = 0x3A // :

const isWhitespace = (c) => { return c === 0x20 || c === 0x0A || c === 0x0D || c === 0x09 }
const isDelimiter  = (c) => { return c === COMMA || c === CLOSE_ARRAY || c === CLOSE_OBJECT || c === COLON || isWhitespace(c) }
const describe     = (c) => { return (c > 0x20 && c < 0x7F) ? `'${String.fromCharCode(c)}'` : `0x${c.toString(16).padStart(2,'0')}` }

// What the scanner expects next in the current DESCENDed container
const EXPECT_VALUE       = 0  // the root value, an array element or a member value
const EXPECT_FIRST_VALUE = 1  // the first array element, or ']'
const EXPECT_FIRST_KEY   = 2  // the first member name, or '}'
const EXPECT_KEY         = 3  // a member name, after ','
const EXPECT_COLON       = 4
const EXPECT_SEPARATOR   = 5  // ',' or the end of the current container
const EXPECT_NOTHING     = 6  // the root value is complete

const CONTAINER = 0
const STRING    = 1
const LITERAL   = 2

class ValueBuilder {

  /*
  **
  ** JSONScanner handler that rebuilds a large array or object from its separately parsed members.
  **
  */

  constructor() {
	this.container = undefined
	this.result = undefined
  }
  
  decide(depth,key,firstByte) {
	return depth === 0 ? JSONScanner.DESCEND : JSONScanner.CAPTURE
  }
  
  open(depth,key,isArray) {
	this.container = isArray ? [] : {}
  }
  
  value(depth,key,value) {
	if (Array.isArray(this.container)) {
	  this.container.push(value)
	}
	else if (key === '__proto__') {
	  // Match JSON.parse(), which creates an own property rather than replacing the prototype
	  Object.defineProperty(this.container,key,{value: value, writable: true, enumerable: true, configurable: true})
	}
	else {
	  this.container[key] = value
	}
  }
  
  close(depth,key,isArray) {
	this.result = this.container
  }
  
  skipped(depth,key,firstByte) {
  }
  
}

class JSONScanner {

  static get DESCEND()                { return 'DESCEND' }
  static get CAPTURE()                { return 'CAPTURE' }
  static get SKIP()                   { return 'SKIP' }

  static get LARGE_VALUE_THRESHOLD()  { return this._LARGE_VALUE_THRESHOLD || 64 * 1024 * 1024 }
  static set LARGE_VALUE_THRESHOLD(v) { this._LARGE_VALUE_THRESHOLD = v }

  // True when the input is not a complete JSON document: a value, member name or container is still open, or nothing has been read.
  get INCOMPLETE()                    { return (this.value !== undefined) || (this.keyFragments !== undefined) || (this.stack.length > 0) || (this.expect !== EXPECT_NOTHING) }

  constructor(handler) {

	this.handler = handler

	this.stack = []                   // The DESCENDed containers: {isArray, key}
	this.expect = EXPECT_VALUE
	this.currentKey = undefined       // The name of the member whose value comes next
	this.position = 0                 // Bytes consumed before the current chunk, for error messages

	this.keyFragments = undefined     // A member name that spans chunks
	this.keyEscaped = false

	this.value = undefined            // The CAPTURE or SKIP in progress
  }

  error(message,offset) {
	return new InvalidJSON(`${message} at byte ${this.position + offset}.`)
  }

  write(chunk) {

	let i = 0
	const length = chunk.length

	while (i < length) {

	  if (this.value !== undefined) {
		i = this.scanValue(chunk,i)
		continue
	  }

	  if (this.keyFragments !== undefined) {
		i = this.scanKey(chunk,i)
		continue
	  }

	  const c = chunk[i]
	  if (isWhitespace(c)) {
		i++
		continue
	  }

	  switch (this.expect) {
		case EXPECT_FIRST_VALUE:
		  if (c === CLOSE_ARRAY) {
			this.closeContainer(c,i)
			i++
			break
		  }
		case EXPECT_VALUE:
		case EXPECT_NOTHING:
		  // Clarinet accepted a second root value after the first (e.g. two arrays concatenated), so continue to accept one.
		  i = this.startValue(chunk,i)
		  break
		case EXPECT_FIRST_KEY:
		  if (c === CLOSE_OBJECT) {
			this.closeContainer(c,i)
			i++
			break
		  }
		case EXPECT_KEY:
		  if (c !== QUOTE) {
			throw this.error(`Expected a member name, found ${describe(c)}`,i)
		  }
		  this.keyFragments = []
		  this.keyEscaped = false
		  i = this.scanKey(chunk,i+1)
		  break
		case EXPECT_COLON:
		  if (c !== COLON) {
			throw this.error(`Expected ':' after member name "${this.currentKey}", found ${describe(c)}`,i)
		  }
		  this.expect = EXPECT_VALUE
		  i++
		  break
		case EXPECT_SEPARATOR:
		  if (c === COMMA) {
			this.expect = this.stack[this.stack.length-1].isArray ? EXPECT_VALUE : EXPECT_KEY
		  }
		  else {
			this.closeContainer(c,i)
		  }
		  i++
		  break
	  }
	}
	this.position += length
  }

  end() {
	// A number or literal at the end of the input has no delimiter to end it
	if (this.value?.kind === LITERAL) {
	  this.finishValue()
	}
  }

  scanKey(chunk,i) {
	const start = i
	for (; i < chunk.length; i++) {
	  const c = chunk[i]
	  if (this.keyEscaped) {
		this.keyEscaped = false
	  }
	  else if (c === BACKSLASH) {
		this.keyEscaped = true
	  }
	  else if (c === QUOTE) {
		this.keyFragments.push(chunk.subarray(start,i))
		const key = Buffer.concat(this.keyFragments).toString('utf8')
		this.keyFragments = undefined
		try {
		  this.currentKey = JSON.parse(`"${key}"`)
		} catch (cause) {
		  throw new InvalidJSON(`Invalid member name "${key}" at byte ${this.position + i}: ${cause.message}`,cause)
		}
		this.expect = EXPECT_COLON
		return i + 1
	  }
	}
	this.keyFragments.push(chunk.subarray(start))
	return i
  }

  startValue(chunk,i) {

	// Returns the index of the first byte after the value (or after the opening bracket of a DESCENDed container)

	const c = chunk[i]
	const depth = this.stack.length
	const key = (depth > 0) && !this.stack[depth-1].isArray ? this.currentKey : undefined

	if (isDelimiter(c)) {
	  throw this.error(`Expected a value, found ${describe(c)}`,i)
	}

	const action = this.handler.decide(depth,key,c)

	if (action === JSONScanner.DESCEND) {
	  if ((c !== OPEN_ARRAY) && (c !== OPEN_OBJECT)) {
		throw this.error(`Expected an array or object, found ${describe(c)}`,i)
	  }
	  const isArray = c === OPEN_ARRAY
	  this.stack.push({isArray: isArray, key: key})
	  this.expect = isArray ? EXPECT_FIRST_VALUE : EXPECT_FIRST_KEY
	  this.handler.open(depth,key,isArray)
	  return i + 1
	}

	this.value = {
	  action    : action
	, depth     : depth
	, key       : key
	, firstByte : c
	, kind      : ((c === OPEN_ARRAY) || (c === OPEN_OBJECT)) ? CONTAINER : (c === QUOTE) ? STRING : LITERAL
	, nesting   : 0
	, inString  : false
	, escaped   : false
	, fragments : []
	, size      : 0
	, splitter  : undefined
	}

	// Scan from the opening byte, so that a CAPTUREd value includes it.
	return this.scanValue(chunk,i)
  }

  scanValue(chunk,i) {

	// Scan the value that starts, or continues, at chunk[i]. Returns the index of the first byte after the value, or chunk.length when the value continues in the next chunk.

	const value = this.value
	const start = i
	let end = -1

	switch (value.kind) {
	  case STRING:
	    // The opening quote sets inString, the closing quote ends the value.
		for (; i < chunk.length; i++) {
		  const c = chunk[i]
		  if (!value.inString) {
			value.inString = true
		  }
		  else if (value.escaped) {
			value.escaped = false
		  }
		  else if (c === BACKSLASH) {
			value.escaped = true
		  }
		  else if (c === QUOTE) {
			end = i + 1
			break
		  }
		}
		break
	  case LITERAL:
		// A number, true, false or null ends at the next delimiter.
		for (; i < chunk.length; i++) {
		  if (isDelimiter(chunk[i])) {
			end = i
			break
		  }
		}
		break
	  default: {
		// Copy the state into locals: this is the loop every row goes through.
		let nesting = value.nesting
		let inString = value.inString
		let escaped = value.escaped
		for (; i < chunk.length; i++) {
		  const c = chunk[i]
		  if (inString) {
			if (escaped) {
			  escaped = false
			}
			else if (c === BACKSLASH) {
			  escaped = true
			}
			else if (c === QUOTE) {
			  inString = false
			}
		  }
		  else if (c === QUOTE) {
			inString = true
		  }
		  else if ((c === OPEN_ARRAY) || (c === OPEN_OBJECT)) {
			nesting++
		  }
		  else if ((c === CLOSE_ARRAY) || (c === CLOSE_OBJECT)) {
			if (--nesting === 0) {
			  end = i + 1
			  break
			}
		  }
		}
		value.nesting = nesting
		value.inString = inString
		value.escaped = escaped
	  }
	}

	if (value.action === JSONScanner.CAPTURE) {
	  this.collect(chunk.subarray(start,end > -1 ? end : chunk.length))
	}

	if (end > -1) {
	  this.finishValue()
	  return end
	}
	return chunk.length

  }

  collect(bytes) {
	const value = this.value
	if (value.splitter) {
	  value.splitter.write(bytes)
	  return
	}
	value.fragments.push(bytes)
	value.size += bytes.length
	if ((value.size > JSONScanner.LARGE_VALUE_THRESHOLD) && (value.kind === CONTAINER)) {
	  // Too large to parse as a whole: parse its members separately. A string or number cannot be split.
	  value.splitter = new JSONScanner(new ValueBuilder())
	  for (const fragment of value.fragments) {
		value.splitter.write(fragment)
	  }
	  value.fragments = undefined
	}
  }

  finishValue() {
	const value = this.value
	this.value = undefined
	this.expect = value.depth === 0 ? EXPECT_NOTHING : EXPECT_SEPARATOR
	if (value.action === JSONScanner.CAPTURE) {
	  this.handler.value(value.depth,value.key,value.splitter ? this.splitValue(value) : this.parseValue(value))
	}
	else {
	  this.handler.skipped(value.depth,value.key,value.firstByte)
	}
  }

  splitValue(value) {
	value.splitter.end()
	if (value.splitter.INCOMPLETE) {
	  throw new InvalidJSON(`Incomplete value ending at byte ${this.position}.`)
	}
	return value.splitter.handler.result
  }
  
  parseValue(value) {
	const content = (value.fragments.length === 1 ? value.fragments[0] : Buffer.concat(value.fragments,value.size)).toString('utf8')
	try {
	  return JSON.parse(content)
	} catch (cause) {
	  throw new InvalidJSON(`${cause.message} [${content.length > 256 ? content.slice(0,256) + '...' : content}]`,cause)
	}
  }

  closeContainer(c,i) {
	const container = this.stack[this.stack.length-1]
	if ((container === undefined) || (c !== (container.isArray ? CLOSE_ARRAY : CLOSE_OBJECT))) {
	  throw this.error(container === undefined ? `Unexpected ${describe(c)}` : `Expected ',' or '${container.isArray ? ']' : '}'}', found ${describe(c)}`,i)
	}
	this.stack.pop()
	const depth = this.stack.length
	this.expect = depth === 0 ? EXPECT_NOTHING : EXPECT_SEPARATOR
	this.handler.close(depth,container.key,container.isArray)
  }

}

export { JSONScanner as default }
