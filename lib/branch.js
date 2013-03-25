/**
 * @fileOverview Definition of Branch (inner node) class of the B+Tree
 * @author LLeo
 * @version 0.0.0
 */

var util = require('util')
  , format = util.format
  , inherits = util.inherits
  , assert = require('assert')
  , Leaf = require('./leaf')
  , Node = require('./node')

/**
 * Constructor for an inner node of B+Tree
 *
 * @param {number} order
 * @param {array} [keys] Array of Key objects in order
 * @param {array} [children] Array of Leaf xor Branch objects in order
 */
exports = module.exports = Branch
function Branch(order, keys, children) {
  Node.call(this, order, keys, children)
  assert.equal(keys.length+1, children.length)

  this.min = Math.ceil(order/2)
  this.max = order
} //constructor

inherits(Branch, Node)


/**
 * This is to small.
 *
 * @returns {boolean}
 */
//Branch.prototype.toSmall = function(){
//  return this.size() < this.min
//} //.toSmall()


/**
 * This is to big.
 * @returns {boolean}
 */
//Branch.prototype.toBig = function(){
//  return this.size() > this.max
//} //.toBig()


/**
 * Update a child Handle that already exists to support COW semantics.
 *
 * @param {Handle} oldHdl
 * @param {Handle} newHdl
 * @returns {Branch} this same Branch object
 * @throw {Error} if oldHdl does not exist
 */
Branch.prototype.setChild = function(oldHdl, newHdl) {
  assert(oldHdl !== null, "Branch.setChild: oldHdl is null")
  assert(newHdl !== null, "Branch.setChild: newHdl is null")

  for (var i=0; i<this.children.length; i++) {
    if ( oldHdl.equals(this.children[i]) ) {
      this.children[i] = newHdl
      break
    }
  }
  if (i == this.children.length)
    throw new Error(format("could not find oldHdl: oldHdl=%j; this=%j", oldHdl, this))

  return this
}


/**
 * Add operation on this Node. Should be followed by a length check and
 * possible split call.
 *
 * The only way a Branch (or Leaf) will be added is if it is the result of
 * a nNode = oNode.split(order). We know nNode and oNode are children of this
 * Node, we want to add nNode into this Node, and nNode > oNode. The node passed
 * as an argument is nNode in this scenario.
 *
 * Key is used to find the placement index in the array of keys and children.
 * The index is used as the place to splice key into this.keys. And index+1
 * is used to splice node into this.children.
 *
 * This is all part of the B+Tree algorithm; trust it.
 *
 * @param {Key} key linear search finds the i'th placement for this Key
 * @param {Node} node spliced into the i+1'th placement
 * @returns {Branch} this object just for shit-n-giggles.
 */
Branch.prototype.add = function(key, node) {
  var i, len

  for (i=0, len=this.keys.length; i<len; i++)
    if (key.cmp(this.keys[i]) < 0) break //key < keys[i]

  this.keys.splice(i, 0, key)
  this.children.splice(i+1, 0, node)

  return this
} //.add()


/**
 * Remove a key and data from a Branch.
 *
 * @param {Key} key
 * @returns {[idx, childId]}
 */
Branch.prototype.remove = function(key, cb) {
  var i, cmp, id
  for (i=0; i<this.keys.length; i++) {
    cmp = key.cmp(this.keys[i])
    if (cmp === 0) {
      id = this.children[i+1]
      this.keys.splice(i, 1)
      this.children.splice(i+1, 1)
      return [i, id]
    }
  }
  console.error("***Branch.remove: failed to find key = %j", key)
  console.error("***Branch.remove: this = %j", this)
  return undefined
} //.remove()


/**
 * Perform a split operation on this Node. It shortens the current Node by half
 * and constructs a new Node out of the other half, then returns the new Node.
 *
 * @returns {[Key, Node]}
 */
Branch.prototype.split = function(){
  var m = Math.floor( this.keys.length / 2 )
    , n = Math.ceil ( this.children.length / 2 )
    , tKeys, tChildren, nNode, pKey

  tKeys = this.keys.splice(m)
  tChildren = this.children.splice(n)

  pKey = tKeys.shift()
  nNode = new Branch(this.order, tKeys, tChildren) //next sibling Branch

  return [pKey, nNode]
} //.split()


Branch.prototype.toString = function(){
  var kStr = this.keys.map(function(k){ return k.toString() }).join(", ")
  var cStr = this.children.map(function(hdl){return hdl.toString()}).join(", ")
  return format("Branch(id=%s, keys=[%s], children=[%s])", this.id, kStr, cStr)
}

//THE END