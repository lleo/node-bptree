// File: mem_store.js
// Abstract:
"use strict";


var assert = require('assert')
  , BpTree = require('./bptree')
  , Leaf   = BpTree.Leaf
  , Branch = BpTree.Branch
  , msgpack = require('msgpack-js')
  , u = require('lodash')
  , format = require('util').format


/**
 * Handle Constructor
 *
 * @constructor
 * @param {Number} id positive integer greater than or equal to 0
 */
function Handle(id) {
  this.id = id
}


/**
 * Test if this Handle equals another
 *
 * @param {Handle} other
 * @return {Boolean}
 */
Handle.prototype.equals = function(other){
  return this.id == other.id
}


/**
 * Create a unique string representation of the Handle
 *
 * @return {String}
 */
Handle.prototype.toString = function(){
  return ""+this.id
}


/**
 * MemStore Constructor.
 *
 * @constructor
 * @param {integer} delay optional argument
 */
exports = module.exports = MemStore
function MemStore(delay) {
  this.nextId = 0
  this.writing = false

  if (delay == undefined || delay == false) //catches null as well
    this.delay = -1 //no delay
  else if (typeof delay == 'number')
    this.delay = delay
  else
    throw new Error("Unknown delay in MemStore.Handle constructor")

  this.buffers = {}
  this.rootHdl = null
}

MemStore.MemStore = MemStore
MemStore.Handle = Handle


MemStore.prototype._nextHandle = function() {
  var hdl = new Handle( this.nextId )
  this.nextId += 1
  return hdl
}


/**
 * Load root Handle
 *
 * @param {Function} cb cb(err, rootHdl)
 */
MemStore.prototype.getRootHandle = function(cb){
  var self = this
  setImmediate(function(){
    cb(null, self.rootHdl)
  })
}


/**
 * Store root Handle in its special place (don't ask:)
 *
 * @param {Handle} rootHdl
 * @param {Function} cb cb(err)
 */
MemStore.prototype.storeRootHandle = function(rootHdl, cb){
  var self = this
  setImmediate(function(){
    self.rootHdl = rootHdl
    cb(null)
  })
}


/**
 * Release a handle space from storage.
 * Trivial in the case of MemStore
 *
 * @param {MemStore.Handle} hdl
 * @param {function} cb cb(err)
 * @api public
 */
MemStore.prototype.release = function release(hdl, cb) {
  var self = this
  setImmediate(function(){
    if ( delete self.buffers[hdl] )
      cb(null)
    else
      cb(new Error(format("MemStore#release: did not contain hdl=%s", hdl)))
  })
}


/**
 * Convert a Handle to a storable plain ole JSON object
 *
 * @param {Handle} hdl
 * @return {Object} plain ole JSON object
 */
MemStore.prototype.handleToJson = function(hdl){
  return { id: hdl.id }
}


/**
 * Convert a plain ole JSON object to a Handle
 *
 * @param {Object} json plain ole JSON object
 * @return {Handle}
 */
MemStore.prototype.handleFromJson = function(json){
  return new Handle(json.id)
}


/**
 * Load a buffer for a given Handle
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, n, hdl) where n is the json object set to store
 * @api public
 */
MemStore.prototype.load = function(hdl, cb) {
  var self = this

  if ( typeof this.buffers[hdl] === 'undefined' ) {
    cb( new Error("typeof this.buffers[hdl] === 'undefined'; hdl ="+hdl) )
    return
  }

  var buf = this.buffers[hdl]
    , n = msgpack.decode(buf)

  if (!u.isPlainObject(n) || typeof n.type == "undefined") {
    cb(new Error(format("WTF! n = %j", n)))
    return
  }

  if ( this.delay < 0 ) {
    setImmediate(function(){
      cb(null, n)
    })
  }
  else {
    setTimeout(function(){
      cb(null, n)
    }, this.delay)
  }
}


/**
 * Store a buffer in a give Handle
 *
 * @param {Node} node
 * @param {Handle} [hdl]
 * @param {function} cb cb(err, hdl)
 * @api public
 */
MemStore.prototype.store = function store(n, hdl, cb) {
  var self = this

  if (typeof hdl == 'function') {
    cb = hdl
    hdl = this._nextHandle()
  }
  else if ( hdl == null && typeof cb == 'function') {
    hdl = this._nextHandle()
  }

  if (this.writing) throw new Error("store already in progress.")

  this.writing = true

  var nodeBuf = msgpack.encode(n)

  if (this.delay < 0) {
    setImmediate(function(){
      self.buffers[hdl] = nodeBuf
      self.writing = false
      cb(null, hdl)
    })
  }
  else {
    setTimeout(function(){
      self.buffers[hdl] = nodeBuf
      self.writing = false
      cb(null, hdl)
    }, this.delay)
  }

  return hdl
} //.store()


/**
 * Flush out all data to the underlying store; noop here
 *
 * @param {Function} cb cb(err)
 */
MemStore.prototype.flush = function(cb){
  //noop
  setImmediate(function(){
    cb(null)
  })
}


/**
 * Close the underlying store; noop here
 *
 * @param {Function} cb cb(err)
 */
MemStore.prototype.close = function(cb){
  //noop
  setImmediate(function(){
    cb(null)
  })
}

//