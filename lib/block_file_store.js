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
  , Stats = require('stats-api')

var bfsStats = Stats().createNameSpace("block-file-store")
bfsStats.createStat('sz_load', Stats.Value, {units: "bytes"})
bfsStats.createStat('sz_store', Stats.Value, {units: "bytes"})
bfsStats.createStat('sz_load_tru', Stats.Value, {units: "bytes"})
bfsStats.createStat('sz_store_tru', Stats.Value, {units: "bytes"})
bfsStats.createHOG('hog sz_load', 'sz_load', Stats.semiBytes)
bfsStats.createHOG('hog sz_store', 'sz_store', Stats.semiBytes)
bfsStats.createHOG('hog sz_load_tru', 'sz_load_tru', Stats.semiBytes)
bfsStats.createHOG('hog sz_store_tru', 'sz_store_tru', Stats.semiBytes)


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
 * Create a BlockFileStore.
 *
 * @param {String} fn filename
 * @param {Function} cb cb(err, bfs)
 */
BlockFileStore.create = function(fn, cb){
  BlockFile.create(fn, function(err, bf){
    if (err) { cb(err); return }
    var bfs = new BlockFileStore(bf)
    cb(null, bfs)
  })
}


/**
 * Ask the underlying stoage if handle can contain Plain ole JSON object n
 *
 * @param {Handle} hdl
 * @param {Object} n Plain ole JSON object
 */
BlockFileStore.prototype.isBigEnough = function(hdl, n){
  assert.ok(hdl instanceof Handle)

  var size = msgpack.encode(n).length

  if (hdl.size() >= size)
    return true
  else
    return false
}


/**
 * Convert a plain ole JSON object to a Handle
 *
 * @param {Object} json plain ole JSON object
 * @return {Handle}
 */
BlockFileStore.prototype.handleFromJson = function(json){
  if (json === null) return null

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
BlockFileStore.prototype.handleToJson = function(hdl){
  assert.ok( this.blockFile.props.equals(hdl.props) )
  return {
    segNum  : hdl.segNum
  , blkNum  : hdl.blkNum
  , spanNum : hdl.spanNum
  }
}


/**
 * Store a Handle in BlockFile's appData
 *
 * @param {Handle} rootHdl
 * @param {Function} cb cb(err)
 */
BlockFileStore.prototype.storeRootHandle = function(rootHdl, cb){
  var appDataBuf, appData, rootHdlJson

//  console.warn("BlockFileStore#storeRootHandle: rootHdl=%s", rootHdl)

  appDataBuf = this.blockFile.getAppData()

  //if the appDataBuf is empty default appData to a new Object
  appData = appDataBuf.length == 0 ? {} : msgpack.decode(appDataBuf)

  //if the appData is undefined or null default to a new Object
  appData = appData == null ? {} : appData

  rootHdlJson = rootHdl ? this.handleToJson(rootHdl) : null

//  console.warn("BlockFileStore#storeRootHandle: rootHdlJson=%s", rootHdlJson)

  appData.rootHdlJson = rootHdlJson

  appDataBuf = msgpack.encode(appData)
  this.blockFile.setAppData(appDataBuf)

  this.blockFile.flush(cb)
//  this.blockFile._writeHeader(cb)
}


/**
 * Load the root Handle from BlockFile's appData
 *
 * @param {Function} cb cb(err, rootHdl)
 */
BlockFileStore.prototype.loadRootHandle = function(cb){
  var appDataBuf, appData, rootHdlJson, rootHdl

  appDataBuf = this.blockFile.getAppData()

  //if the appDataBuf is empty default appData to a new Object
  appData = appDataBuf.length == 0 ? {} : msgpack.decode(appDataBuf)

  //if the appData is undefined or null default to a new Object
  appData = appData == null ? {} : appData


  rootHdlJson = appData.rootHdlJson || null
  rootHdl = this.handleFromJson(rootHdlJson)

//  console.warn("BlockFileStore#loadRootHandle: rootHdlJson=%j; rootHdl=%s;"
//              , rootHdlJson, rootHdl)

  setImmediate(function(){
    cb(null, rootHdl)
  })
}


/**
 * Reserve a new Handle to store a given Plain-Ole-JSON object
 *
 * @param {Object} n Plain-Ole-JSON object
 * @return {Handle}
 */
BlockFileStore.prototype.reserve = function(n){
  var buf, hdl

  buf = msgpack.encode(n)

  //  hdl = this.blockFile._reserve(buf.length)

  hdl = this.blockFile._reserve(1)
  assert.ok( hdl.size() >= buf.length )

  return hdl
}


/**
 * Release the space allocated for a `hdl`.
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err)
 */
BlockFileStore.prototype.release = function(hdl, cb){
  var self = this

  setImmediate(function(){
    var ok = self.blockFile.release(hdl)
    if (!ok) {
      cb(new Error(format("BlockFileStore#release: underlying BlockFile failed to release Handle=%s", hdl)))
      return
    }

    cb(null)
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

    bfsStats.get('sz_load').set(len)
    bfsStats.get('sz_load_tru').set(buf.length)

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

  bfsStats.get('sz_store').set(msgBuf.length)
  bfsStats.get('sz_store_tru').set(buf.length)

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