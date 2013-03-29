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
function Branch(order, keys, children) {
  Node.call(this, order, keys, children)
  assert.equal(keys.length+1, children.length)

  this.min = Math.ceil(order/2)
  this.max = order
} //constructor

inherits(Branch, Node)

Branch.Branch = Branch

exports = module.exports = Branch

//DEL/**
//DEL * Update a child Handle that already exists to support COW semantics.
//DEL *
//DEL * @param {Handle} oldHdl
//DEL * @param {Handle} newHdl
//DEL * @returns {Branch} this same Branch object
//DEL * @throw {Error} if oldHdl does not exist
//DEL */
//DELBranch.prototype.setChild = function(oldHdl, newHdl) {
//DEL  assert(oldHdl !== null, "Branch.setChild: oldHdl is null")
//DEL  assert(newHdl !== null, "Branch.setChild: newHdl is null")
//DEL
//DEL  for (var i=0; i<this.children.length; i++) {
//DEL    if ( oldHdl.equals(this.children[i]) ) {
//DEL      this.children[i] = newHdl
//DEL      break
//DEL    }
//DEL  }
//DEL  if (i == this.children.length)
//DEL    throw new Error(format("could not find oldHdl: oldHdl=%j; this=%j", oldHdl, this))
//DEL
//DEL  return this
//DEL}


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
 * @param {Node} id spliced into the i+1'th placement
 * @returns {Branch} this object just for shit-n-giggles.
 */
Branch.prototype.add = function(key, id) {
  var i, len, cmp, errstr

  for (i=0, len=this.keys.length; i<len; i++) {
    cmp = key.cmp(this.keys[i])

    assert(cmp !== 0, format(
      "trying to add a key(%s) that already exists in Branch; cur=%s, new=%s",
      key, this.children[i+1], id))

    if (cmp < 0) //key < keys[i]
      break
  }

  this.keys.splice(i, 0, key)
  this.children.splice(i+1, 0, id)

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
  console.error("***Branch.remove: failed to find key = %s", key)
  console.error("***Branch.remove: this = %s", this)
  return undefined
} //.remove()


/**
 * Remove a id and key from a Branch.
 * Mustn't & Won't remove the first child
 *
 * @param {Key} key
 * @returns {[idx, key]}
 */
Branch.prototype.removeById = function(id, cb) {
  var i, key
  for (i=1; i<this.children.length; i++) {
    if ( id.equals(this.children[i]) ) {
      key = this.keys[i-1]
      this.keys.splice(i-1, 1)
      this.children.splice(i, 1)
      return [i, key]
    }
  }
  console.error("***Branch.remove: failed to find id = %s", key)
  console.error("***Branch.remove: this = %s", this)
  return undefined
} //.removeById()


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
  return format("Branch(id=%s, min=%d, max=%d, keys=[%s], children=[%s])"
               , this.id, this.min, this.max, kStr, cStr)
}

//THE END