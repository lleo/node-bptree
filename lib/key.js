
exports = module.exports = Key
/**
 * Key Constructor
 *
 * @constructor
 * @param {object} d
 */
function Key(d) {
  this.d = d
}


/**
 * Read the length of the next packed key.
 *   packed length is encoded as a UInt32BE aka 4 bytes
 *
 * @param {Buffer} buffer
 * @param {number} length
 * @returns {number}
 */
Key.packedLength = function(buffer, offset){
  return buffer.readUInt32BE(offset)
}


/**
 * Decode key from a Buffer.
 *
 * @param {Buffer} buffer
 * @param {number} offset remember offset MUST be +4 from Key.packedLength()
 * @param {length} length value returned by Key.packedLength()
 * @returns {Key}
 */
Key.unpack = function(buffer, offset){
  throw new Error("abstract base class method")
}


/**
 * Compare this key with another, returning -1, 0, or 1 for less-than, equal,
 * or greater-than.
 *
 * @param {Key} other
 */
Key.prototype.cmp = function(other){
  throw new Error("abstract base class instance method")
}


/**
 * Calculate the number of bytes to encode this key.
 *
 * @returns {number} in bytes
 */
Key.prototype.packLength = function(){
  throw new Error("abstract base class instance method")
}


/**
 * Encode key into a Buffer.
 *
 * @param {Buffer} buffer
 * @param {number} offset offset into buffer to start encoding
 */
Key.prototype.pack = function(buffer, offset){
  throw new Error("abstract base class instance method")
}


/**
 * Output a string representation of the Key
 */
Key.prototype.toString = function(){
  throw new Error("abstract base class instance method")
}


//