
var util = require('util')
  , inherits = util.inherits
  , format = util.format
  , assert = require('assert')
  , Key = require('../key')


exports = module.exports = Utf8Str
function Utf8Str(s) {
  assert.equal(typeof s, 'string')
  Key.call(this)
  this.d = s
}

inherits(Utf8Str, Key)


Utf8Str.unpack = function(buffer, offset){
  var len = buffer.readUInt32BE(offset)
  return buffer.toString('utf8', offset, offset+len)
}


Utf8Str.prototype.cmp = function(other){
  assert.ok(typeof other == typeof this)
  return this.d.length > other.d.length ? 1 :
    this.d.length < other.d.length ? -1 :
    this.d > other.d ? 1 :
    this.d < other.d ? -1 : 0

    //return this.d===other.d ? 0 : this.d < other.d ? -1 : 1
}


Utf8Str.prototype.packLength = function(){
  return 4 + Buffer.byteLength(this.d, 'utf8')
}


Utf8Str.prototype.pack = function(buffer, offset){
  var len = Buffer.byteLength(this.d, 'utf8')

  buffer.writeUInt32BE(len, offset)
  buffer.write(this.d, offset+4, len, 'utf8')
}


Utf8Str.prototype.toString = function(){
  return '"'+this.d+'"'
}


//