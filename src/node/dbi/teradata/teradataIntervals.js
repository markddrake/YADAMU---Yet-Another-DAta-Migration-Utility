/*
**
** Teradata's INTERVAL and PERIOD types.
**
** teradatasql returns an INTERVAL as Teradata's text form ("   45 12:30:15.123456", "   -0-01") and a PERIOD as "begin,end".
** YADAMU exchanges intervals as ISO 8601 durations (P45DT12H30M15.123456S), as the Oracle and Postgres drivers do, and a PERIOD as JSON in
** the form the Postgres driver uses for ranges. A PERIOD includes its beginning and excludes its end.
**
** INTERVAL YEAR ('YR') also stores YADAMU's YEAR type (MySQL's YEAR), but it is a duration, so it is exported as one (P2026Y, -P1Y). A YEAR
** written to it arrives as a number, and stays a number.
**
*/

const INTERVAL_FIELDS = Object.freeze({
  YR : ['YEAR']
, YM : ['YEAR','MONTH']
, MO : ['MONTH']
, DY : ['DAY']
, DH : ['DAY','HOUR']
, DM : ['DAY','HOUR','MINUTE']
, DS : ['DAY','HOUR','MINUTE','SECOND']
, HR : ['HOUR']
, HM : ['HOUR','MINUTE']
, HS : ['HOUR','MINUTE','SECOND']
, MI : ['MINUTE']
, MS : ['MINUTE','SECOND']
, SC : ['SECOND']
})

const PERIOD_ELEMENTS = Object.freeze({
  PD : (precision) => `DATE`
, PT : (precision) => `TIME(${precision})`
, PZ : (precision) => `TIME(${precision}) WITH TIME ZONE`
, PS : (precision) => `TIMESTAMP(${precision})`
, PM : (precision) => `TIMESTAMP(${precision}) WITH TIME ZONE`
})

const MAX_LEADING_PRECISION  = 4
const MAX_FRACTION_PRECISION = 6

// A negative duration has a leading minus sign (Oracle, Teradata) or negative fields (Postgres: P-1Y-2M, PT-1H-0.5S). Postgres's fields can differ in sign.

const ISO_DURATION = /^(-)?P(?:(-?\d+)Y)?(?:(-?\d+)M)?(?:(-?\d+)W)?(?:(-?\d+)D)?(?:T(?:(-?\d+)H)?(?:(-?\d+)M)?(?:(-?\d*(?:\.\d+)?)S)?)?$/

const MICROS = Object.freeze({ DAY: 86400000000n, HOUR: 3600000000n, MINUTE: 60000000n, SECOND: 1000000n })

class TeradataIntervals {

  static isInterval(dataType)  { return INTERVAL_FIELDS.hasOwnProperty(dataType) }

  static isPeriod(dataType)    { return PERIOD_ELEMENTS.hasOwnProperty(dataType) }

  static leadingPrecision(sizeConstraint) {
	return Math.min(sizeConstraint?.[0] ?? MAX_LEADING_PRECISION, MAX_LEADING_PRECISION)
  }

  static fractionPrecision(sizeConstraint) {
	return Math.min(sizeConstraint?.[1] ?? MAX_FRACTION_PRECISION, MAX_FRACTION_PRECISION)
  }

  static intervalDDL(dataType,sizeConstraint) {
	const fields = INTERVAL_FIELDS[dataType]
	const leading = this.leadingPrecision(sizeConstraint)
	const fraction = this.fractionPrecision(sizeConstraint)
	const last = fields[fields.length - 1]
	switch (true) {
	  case (fields.length === 1) && (last === 'SECOND'):
	    return `INTERVAL SECOND(${leading},${fraction})`
	  case (fields.length === 1):
	    return `INTERVAL ${fields[0]}(${leading})`
	  case (last === 'SECOND'):
	    return `INTERVAL ${fields[0]}(${leading}) TO SECOND(${fraction})`
	  default:
	    return `INTERVAL ${fields[0]}(${leading}) TO ${last}`
	}
  }

  static periodDDL(dataType,sizeConstraint) {
	const precision = Math.min(sizeConstraint?.[0] ?? MAX_FRACTION_PRECISION, MAX_FRACTION_PRECISION)
	return `PERIOD(${PERIOD_ELEMENTS[dataType](precision)})`
  }

  // "   45 12:30:15.123456" => "P45DT12H30M15.123456S"

  static intervalToISO(dataType,text) {
	let value = text.trim()
	const negative = value.startsWith('-')
	value = negative ? value.substring(1) : value
	const fields = INTERVAL_FIELDS[dataType]
	const values = value.split(fields[0] === 'YEAR' ? '-' : /[ :]/)
	const parts = Object.fromEntries(fields.map((field,idx) => { return [field,values[idx]] }))
	let iso = 'P'
	iso += parts.YEAR !== undefined ? `${parseInt(parts.YEAR)}Y` : ''
	iso += parts.MONTH !== undefined ? `${parseInt(parts.MONTH)}M` : ''
	iso += parts.DAY !== undefined ? `${parseInt(parts.DAY)}D` : ''
	if ((parts.HOUR !== undefined) || (parts.MINUTE !== undefined) || (parts.SECOND !== undefined)) {
	  iso += 'T'
	  iso += parts.HOUR !== undefined ? `${parseInt(parts.HOUR)}H` : ''
	  iso += parts.MINUTE !== undefined ? `${parseInt(parts.MINUTE)}M` : ''
	  if (parts.SECOND !== undefined) {
		const [whole,fraction = ''] = parts.SECOND.split('.')
		const digits = fraction.replace(/0+$/,'')
		iso += `${parseInt(whole)}${digits.length > 0 ? `.${digits}` : ''}S`
	  }
	}
	return `${negative ? '-' : ''}${iso}`
  }

  // "P45DT12H30M15.123456S" => "45 12:30:15.123456" in the form of the column's type. A value that isn't an ISO duration is passed on as it is.

  static intervalFromISO(dataType,value,sizeConstraint) {
	const components = (typeof value === 'string') && value.trim().match(ISO_DURATION)
	if (!components) {
	  return value
	}
	const [, negative, years = '0', months = '0', weeks = '0', days = '0', hours = '0', minutes = '0', seconds = '0'] = components
	const direction = negative ? -1n : 1n
	const fields = INTERVAL_FIELDS[dataType]
	if (fields[0] === 'YEAR' || fields[0] === 'MONTH') {
	  const totalMonths = direction * ((BigInt(years) * 12n) + BigInt(months))
	  const sign = totalMonths < 0n ? '-' : ''
	  const absMonths = totalMonths < 0n ? -totalMonths : totalMonths
	  switch (true) {
		case (dataType === 'MO'):
		  return `${sign}${absMonths}`
		case (dataType === 'YR') && (absMonths % 12n !== 0n):
		  // An INTERVAL YEAR has no months. The value is passed on as it is, and Teradata rejects it, rather than losing them.
		  return value
		case (dataType === 'YR'):
		  return `${sign}${absMonths / 12n}`
		default:
		  return `${sign}${absMonths / 12n}-${String(absMonths % 12n).padStart(2,'0')}`
	  }
	}
	const [whole = '', fraction = ''] = seconds.replace('-','').split('.')
	const secondMicros = (BigInt(whole || '0') * MICROS.SECOND + BigInt(fraction.padEnd(6,'0').substring(0,6))) * (seconds.startsWith('-') ? -1n : 1n)
	let micros = direction * ((((BigInt(weeks) * 7n + BigInt(days)) * 24n + BigInt(hours)) * 60n + BigInt(minutes)) * 60n * MICROS.SECOND + secondMicros)
	const sign = micros < 0n ? '-' : ''
	micros = micros < 0n ? -micros : micros
	const precision = this.fractionPrecision(sizeConstraint)
	const parts = fields.map((field,idx) => {
	  // The leading field holds the whole of the value above the next field, and has no leading zeros
	  const amount = idx === 0 ? micros / MICROS[field] : (micros % MICROS[fields[idx - 1]]) / MICROS[field]
	  const digits = idx === 0 ? `${amount}` : String(amount).padStart(2,'0')
	  return ((field === 'SECOND') && (precision > 0)) ? `${digits}.${String(micros % MICROS.SECOND).padStart(6,'0').substring(0,precision)}` : digits
	})
	// DAY is followed by a space, every other field by a colon
	return sign + parts.reduce((text,part,idx) => { return `${text}${fields[idx - 1] === 'DAY' ? ' ' : ':'}${part}` })
  }

  // "2026-10-19 08:00:00.000000,2026-10-20 17:30:00.123456" => {"lowerBound":"2026-10-19T08:00:00.000000","upperBound":"2026-10-20T17:30:00.123456",...}

  static periodToJSON(dataType,text) {
	const [lowerBound,upperBound] = text.split(',').map((bound) => { return bound.trim().replace(' ','T') })
	return JSON.stringify({lowerBound, upperBound, hasLowerBound: true, hasUpperBound: true, includeLowerBound: true, includeUpperBound: false})
  }

  // A range (JSON, or an object), or Teradata's "begin,end", => "('begin', 'end')", which Teradata casts to a PERIOD.

  static periodFromJSON(dataType,value) {
	let bounds
	switch (true) {
	  case (typeof value === 'object'):
		bounds = [value.lowerBound, value.upperBound]
		break
	  case (typeof value === 'string') && value.trim().startsWith('{'):
		const range = JSON.parse(value)
		bounds = [range.lowerBound, range.upperBound]
		break
	  case (typeof value === 'string'):
		bounds = value.split(',')
		break
	  default:
		return value
	}
	const toTeradata = (bound) => { return String(bound).trim().replace('T',' ').replace(/Z$/,'+00:00') }
	return `('${toTeradata(bounds[0])}', '${toTeradata(bounds[1])}')`
  }
}

export { TeradataIntervals as default }
