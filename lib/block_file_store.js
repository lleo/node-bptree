// File: block_file_store.js
// Abstract:
"use strict";

var assert = require('assert')
  , util = require('util')
  , inherits = util.inherits
  , format = util.format
  , u = require('lodash')
  , BlockFile = require('block-file')
  , msgpack = require('msgpack-js')

var Handle = BlockFile.Handle
  , Props = BlockFile.Props


/**
 * BlockFileStore Constructor
 *
 * @constructore
 * @param {BlockFile} blockFile the underlying store
 */
module.exports = exports = BlockFileStore
function BlockFileStore(blockFile){
  this.blockFile = blockFile //HASA relationship
}

BlockFileStore.Handle = Handle

/**
 * Open and/or create a BlockFileStore.
 *
 * @param {String} fn filename
 * @param {Function} cb cb(err, bfs)
 */
BlockFileStore.open = function(fn, cb){
  BlockFile.open(fn, function(err, bf){
    if (err) { cb(err); return }
    var bfs = new BlockFileStore(bf)
    cb(null, bfs)
  })
}


/**
 * Convert a plain ole JSON object to a Handle
 *
 * @param {Object} json plain ole JSON object
 * @return {Handle}
 */
BlockFileStore.prototype.handleFromJSON = function(json){
  return new Handle( json.segNum
                   , json.blkNum
                   , json.spanNum
                   , this.blockFile.props )
}

/**
 * Convert a Handle to a JSON Object
 *
 * @param {Handle} hdl
 * @return {Object} a Plain ole JSON object
 */
BlockFileStore.prototype.handleToJSON = function(hdl){
  assert.ok( this.blockFile.props.equals(hdl.props) )
  return {
    segNum  : hdl.segNum
  , blkNum  : hdl.blkNum
  , spanNum : hdl.spanNum
  }
}


/**
 * Release the space allocated for a `hdl`.
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err)
 */
BlockFileStore.prototype.release = function(hdl, cb){
  var ok = this.blockFile.release(hdl)
  setImmediate(function(){
    if (ok)
      cb(null)
    else
      cb(new Error(format("BlockFileStore#release: underlying BlockFile failed to release Handle=%s", hdl)))
  })
}

/**
 * Load a buffer for the data stored in a given Handle.
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err, n)
 */
BlockFileStore.prototype.load = function(hdl, cb){
  this.blockFile.load(hdl, function(err, buf){
    if (err) { cb(err); return }

    var len = buf.readUInt32BE(0)
      , msgBuf = buf.slice(4, 4+len)
      , n = msgpack.decode(msgBuf)

    if (!u.isPlainObject(n) || typeof n.type == "undefined") {
      cb(new Error(format("WTF! n = %j", n)))
      return
    }

    cb(null, n)
  })
}

/**
 * Store a buffer. If a Handle is given, store buffer in that location, else
 * allocate a location for the buffer and store it. Either way, the location
 * of the stored buffer is returned in the callback.
 *
 * @param {Object} json Plain ole JSON object
 * @param {Handle} [hdl]
 * @param {Function} cb cb(err, hdl)
 */
BlockFileStore.prototype.store = function(json, hdl, cb){
  var msgBuf = msgpack.encode(json)
    , buf = new Buffer(4+msgBuf.length) //uint32BE + msg

  buf.writeUInt32BE(msgBuf.length, 0)
  msgBuf.copy(buf, 4)

  this.blockFile.store(buf, hdl, cb)
}


/**
 * Flush out all data to the underlying store
 *
 * @param {Function} cb cb(err)
 */
BlockFileStore.prototype.flush = function(cb){
  this.blockFile.flush(cb)
}


/**
 * Close the underlying store
 *
 * @param {Function} cb cb(err)
 */
BlockFileStore.prototype.close = function(cb){
  this.blockFile.close(cb)
}

//