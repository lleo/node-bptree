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

function Handle(id) {
  this.id = id
}

Handle.fromJSON = function(h){
  return new Handle(h.id)
}

Handle.toJSON = function(hdl){
  return { "id" : hdl.id }
}

Handle.prototype.equals = function(other){
  return this.id == other.id
}

Handle.prototype.toString = function(){
  return ""+this.id
}


/**
 * MemStore Constructor.
 *
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
}

MemStore.MemStore = MemStore
MemStore.Handle = Handle

MemStore.prototype.nextHandle = function() {
  var hdl = new Handle( this.nextId )
  this.nextId += 1
  return hdl
}


/**
 * Reserve a handle for a given sized buffer.
 *
 * @param {integer} sz
 * @param {function} cb cb(err, hdl)
 * @api public
 */
MemStore.prototype.reserve = function reserve(sz, cb) {
  var self = this
  //ignores sz
  setImmediate(function(){
    var hdl = self.nextHandle()
    self.buffers[hdl] = sz //so delete self.buffers[hdl] returns true
    cb(null, hdl)
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
 * Load a buffer for a given Handle
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, buf)
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
    hdl = this.nextHandle()
  }
  else if ( hdl == null && typeof cb == 'function') {
    hdl = this.nextHandle()
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

//