/**
 * @fileOverview Definition of Inner Node class
 * @author LLeo
 * @version 0.0.0
 */

var assert = require('assert')

/**
 * Constructor for an base class node of B+Tree
 *
 * @constructor
 * @param {number} order
 * @param {Array(Key)} keys Array of Key objects in order
 * @param {Array(Node)} children Array of Node objects in order
 */
function Node(order, keys, children, keyCmp) {
  assert.ok(keys instanceof Array)
  assert.ok(children instanceof Array)

  this.order = order
  this.min = NaN
  this.max = NaN
  this.hdl = null

  //storable data
  this.keys = keys
  this.children = children

  this.keyCmp = keyCmp

  Node.CNT += 1
}

Node.Node = Node

exports = module.exports = Node

Node.CNT = 0

/**
 * How wide is this Node with respect to order
 *
 * @returns {number}
 */
Node.prototype.size = function(){
  return this.children.length
} //.size()


/**
 * This is to small.
 *
 * @returns {boolean}
 */
Node.prototype.toSmall = function(){
  assert.ok(!isNaN(this.min))
  return this.size() < this.min
}


/**
 * This is to big.
 * @returns {boolean}
 */
Node.prototype.toBig = function(){
  assert.ok(!isNaN(this.max))
  return this.size() > this.max
}


/**
 * NOT IMPLEMENTED
 * Add operation on this Node. Should be followed by a length check and
 * possible split call.
 *
 * This is all part of the B+Tree algorithm; trust it.
 *
 * @param {Key} key linear search finds the i'th placement for this Key
 * @param {Leaf|Node} node spliced into the i+1'th placement
 * @returns {Node} this object just for shit-n-giggles.
 */
Node.prototype.add = function(key, node) {
  throw new Error("not implemented")
} //.add()


/**
 * NOT IMPLEMENTED
 * Remove a key and child from node
 *
 */
Node.prototype.remove = function(key) {
  throw new Error("not implemented")
} //.remove()


/**
 * NOT IMPLEMENTED
 * Perform a split operation on this Node.
 *
 * @returns {[Key, Node]}
 */
Node.prototype.split = function(){
  throw new Error("not implemented")
} //.split()


/**
 * Perform a merge operation on this Node and its immediate right sibling.
 *
 * @param {Node} sib
 * @returns {Node} this node
 */
Node.prototype.merge = function(sib){
  assert.ok(this.constructor === sib.constructor)
  Array.prototype.push.apply(this.keys, sib.keys)
  Array.prototype.push.apply(this.children, sib.children)
  return this
} //.merge()



//THE END