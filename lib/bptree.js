/**
 * @fileOverview Top level B+Tree class with BlockFile block store
 * @author LLeo
 * @version 0.0.0
 */

var assert = require('assert')
  , util = require('util')
  , format = util.format
  , async = require('async')
  , u = require('lodash')
  , Node = require('./node')
  , Branch = require('./branch')
  , Leaf = require('./leaf')
  , strOps = require('./str_ops')
  , strCmp = strOps.cmp
  , TrivialStore = require('./trivial_store')
  , MemStore = require('./mem_store')
  , msgpack = require('msgpack-js')


/**
 * B+Tree data struture
 *
 * @param {number} order
 * @param {Function} keyType
 */
exports = module.exports = BpTree
function BpTree(order, keyCmp, storage) {
  assert(typeof order === 'number', "order must be an number")
  assert(order%1===0, "order must be an integer")
  assert(order > 1, "order must be greater than 1")

  this.order = order
  if (keyCmp == null || keyCmp == 'string')
    this.keyCmp = strCmp
  else
    this.keyCmp = keyCmp

  if (typeof storage == 'undefined')
    this.storage = new TrivialStore()
  else
    this.storage = storage

  assert.ok(typeof this.keyCmp == 'function')
  assert.ok(u.isObject(this.storage))

  this.root = null
  this.writing = false
} //constructor

BpTree.Node = Node
BpTree.Leaf = Leaf
BpTree.Branch = Branch


/**
 * Insert a Key-Value pair in the B+Tree
 *
 * @param {Key} key
 * @param {object} value
 * @param {Function} cb cb(err)
 */
BpTree.prototype.put = function(key, value, cb){
  var self = this

  assert(!u.isUndefined(key)  , "BpTree#put: key of undefined is not allowed")
  assert(!u.isUndefined(value), "BpTree#put: value of undefined is not allowed")

  if (this.root === null) { //empty tree
    //create leaf node
    var newRoot = new Leaf(this.order, [key], [value], this.keyCmp)
    this.replaceRoot(newRoot, [], cb)
    return
  }

  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    self.insertIntoLeaf(leaf, path, key, value, function(err){
      if (err) { cb(err); return }
      cb(null)
    }) //insertIntoLeaf
  }) //findLeaf

} //.put()


/**
 * Retrieve data for a given Key w/o deleting it.
 *
 * @param {Key} key
 * @param {Function} cb cb(err, data)
 * @returns {object} data value assosiated with key.
 */
BpTree.prototype.get = function(key, cb){
  var self = this
  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }
    cb(null, leaf.get(key))
    return
  })
} //.get()


/**
 * Remove a Key-Value pair from the B+Tree
 *
 * @param {Key} key
 * @param {Function} cb cb(err, data)
 * @returns {object} data value assosiated with key.
 */
BpTree.prototype.del = function(key, cb){
  var self = this

  this.findLeaf(key, function(err, leaf, path){
    var idx, data
    if (err) { cb(err); return }

    data = leaf.get(key)
    if ( u.isUndefined(data) ) {
      cb(null) //undefined means not found
      return
    }

    //self.removeFromLeaf(leaf, key, path, function(err, deadHdls){
    self.removeFromLeaf(leaf, key, path, function(err, deadHdls){
      if (err) { cb(err); return }
      //do something with deadHdls
      cb(null, data)
    })
  })
} //.del()


/**
 * Visit each node pre-order traversal.
 *
 * @param {Function} visit visit(node, isLeaf, depth)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.traversePreOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodePreOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.parallel(
        node.children.map(function(hdl){
          return function(pcb){ self.load(hdl, pcb) }
        })
      , function(err, childs){
          if (err) { done(err); return }
          async.parallel(
            childs.map(function(n){
              return function(pcb){ traverseNodePreOrder(n, depth+1, pcb) }
            })
          , function(err) {
              if (err) { nodeDone(err); return }
              nodeDone(null)
            }) //async.parallel traverse each node
        }) //async.parallel hdl
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true, depth) )
      nodeDone(null)
    }
  }

  if (this.root === null) {
    done(null, [])
    return
  }
  traverseNodePreOrder(this.root, 0, function(err){
    var hasDone = typeof done === 'function'
    if (err) {
      if (hasDone) { done(err); return }
      else throw new Error(err)
    }
    hasDone && done(null, u.flatten(res))
  })
} //.traversePreOrder()


/**
 * Visit each node in-order traversal.
 *
 * @param {Function} visit visit(node, isLeaf, depth)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.traverseInOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodeInOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.series(
        node.children.map(function(hdl, i){
          return function(next){
            self.load(hdl, function(err, child){
              traverseNodeInOrder(child, depth+1, next) })
          }
        })
      , nodeDone) //async.series hdl
    }
    else if (node instanceof Leaf){
      res.push( visit(node, true, depth) )
      nodeDone(null)
    }
  }

  if (this.root === null) {
    done(null, [])
    return
  }
  traverseNodeInOrder(this.root, 0, function(err){
    if (done && typeof done === 'function') {
      if (err) { done(err); return }
      done(null, u.flatten(res))
    }
    else {
      if (err) throw new Error(err)
    }
  })
} //.traverseInOrder()


/**
 * Visit each key/data pair in order.
 *
 * @param {Function} visit visit(key, data)
 * @param {Function} [done] done(err, res)
 */
BpTree.prototype.forEach = function(visit, done){
  this.traverseInOrder(function(node){
    if (node instanceof Leaf) {
      node.forEach(visit)
    }
  }, done)
} //.forEach()


/**
 * Search BpTree for a leaf node which would have a given key
 *
 * @param {Key} key
 * @param {Function} cb cb(err, leaf, path) path isa Array of Branch objects
 */
BpTree.prototype.findLeaf = function(key, cb){
  if (this.root === null) { cb(null, null); return }

  var self = this
    , path = []

  function find(err, node) {
    if (err) { cb(err); return }

    if (node instanceof Branch) {
      //node isa Branch so node.children contains Hdls
      path.push(node)
      for (var i=0; i<node.keys.length; i++)
        if ( self.keyCmp(key, node.keys[i]) < 0 ) break

      self.load(node.children[i], find)
      return
    }
    assert.ok(node instanceof Leaf)
    cb(null, node, path)
  }

  find(null, this.root)
} //.findLeaf()


/**
 * Insert a Key and child into a Node of this BpTree.
 *
 * @param {Node} node inner Node to insert key and Node|Leaf into.
 * @param {Array(Branch)} path array of Branch objects from root to parent
 * @param {Key} key
 * @param {Node} cNode
 * @param {Function} cb cb(err)
 */
BpTree.prototype.insertIntoLeaf = function(leaf, path_, key, child, cb) {
  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  var self = this
    , path = u.clone(path_)
    , deadHdls = []

  leaf.add(key, child)
  if ( !leaf.toBig() ) { //no split is necessary, just store
    this.storeLeaf(leaf, path, cb)
    return
  }
  //node.toBig() needs node.split()

  var lLeaf = leaf
    , pair  = leaf.split()
    , pKey  = pair[0]
    , rLeaf = pair[1]

  this.storeLeafSplit(lLeaf, pKey, rLeaf, path, cb)
} //.insert()


/**
 * A single leaf node needs be stored and its parent needs to have the handle
 * update. There is no modification of size or keys and all parents are Branch
 * nodes. Further, given no modifications of size means this update can be
 * cascaded up to the root.
 *
 * @param {Leaf} leaf leaf object that must be stored
 * @param {Array} path array of branch nodes being the path to root
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeLeaf = function(leaf, path, cb){
  this.storeSimplePath(leaf, path, [], cb)
}


/**
 * Store and node and replace the new handle in parent, then bubble up the
 * changes to the parent.
 *
 * @param {Node} node
 * @param {Array} path array of branch nodes being the path to root
 * @param {Array} deadHdls array of newly stored Handles
 * @param {Function} cb cb(err, deadHdls)
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeSimplePath = function(node, path, deadHdls, cb){
  var self = this

  assert(path instanceof Array, "path !instanceof Array")

//  console.warn("BpTree#storeSimplePath: node=%s; path=%j", node
//              , path.map(function(p){ return p.toString() }) )

  var oldHdl = node.hdl

  this.store(node, function(err, newHdl){
    if (err) { cb(err); return }

    if (oldHdl && !newHdl.equals(oldHdl)) deadHdls.push(oldHdl)

    if ( path.length > 0 ) {
      var parent = path.pop() //top is last
      //RULE: if parent exists then leaf must have been stored before
      //RULE: if a node was stored before it has an non-null hdl;
      //        aka oldHdl !undefined

      if (parent.dirty || !oldHdl.equals(newHdl)) {
        if (oldHdl) parent.updateHdl(oldHdl, newHdl)

        self.storeSimplePath(parent, path, deadHdls, cb)

        return
      } //if newHdl needs to be stored

      //parent is not dirty and the current node has not changed its
      // handle (so the parent does not need to be updated).
      //So just clean the handles.
      setImmediate(function(){
        self.cleanHandles(deadHdls, cb)
      })
    } //if parent isnot undefined
    else {
      self.replaceRoot(node, deadHdls, cb)
    }
  })
}


/**
 * Store a split leaf (lLeaf, pKey, rLeaf) and bubble up the changes
 *
 * @param {Node} lLeaf
 * @param {Key} pKey
 * @param {Node} rLeaf
 * @param {Array} path array of branch nodes being the path to root
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeLeafSplit = function(lLeaf, pKey, rLeaf, path, cb){
  this.storeSplitPath(lLeaf, pKey, rLeaf, path, [], cb)
}


/**
 * Store a split node (lNode, pKey, rNode) and bubble up the changes
 *
 * @param {Node} lNode
 * @param {Key} pKey
 * @param {Node} rNode
 * @param {Array} path array of branch nodes being the path to root
 * @param {Array} deadHdls array of dead Handles
 * @param {Function} cb cb(err)
 */
BpTree.prototype.storeSplitPath = function(lNode, pKey, rNode, path, deadHdls, cb){
  var self = this

  var lHdl = lNode.hdl
    , rHdl = rNode.hdl

  async.series(
    [ function(scb){
        self.store(lNode, function(err, hdl){
          if (err) { cb(err); return }
          scb(null, hdl)
        })
      }
    , function(scb){
        self.store(rNode, function(err, hdl){
          if (err) { cb(err); return }
          scb(null, hdl)
        })
      }
    ]
  , function(err, res){
      if (lHdl && !lHdl.equals(lNode.hdl)) deadHdls.push(lHdl)
      if (rHdl && !rHdl.equals(rNode.hdl)) deadHdls.push(rHdl)

      if ( path.length == 0 ) {
        var newRoot = new Branch(self.order, [pKey], [lNode.hdl, rNode.hdl], self.keyCmp)
        self.replaceRoot(newRoot, deadHdls, cb)
        return
      }

      var parent = path.pop()

      parent.add(pKey, lNode.hdl, rNode.hdl)

      if ( !parent.toBig() ) {
        self.storeSimplePath(parent, path, deadHdls, cb)
        return
      }
      //parent.toBig needs split

      var pair = parent.split()
        , nKey  = pair[0]
        , nNode = pair[1]

      //                  lNode   pKey  rNode
      self.storeSplitPath(parent, nKey, nNode, path, deadHdls, cb)
    })

}


/**
 * Remove a child from a Leaf by Key
 * and propengate changes up the tree if needed.
 *
 * @param {Leaf} leaf
 * @param {Key} key
 * @param {Array} path
 * @param {Function} cb cb(err)
 *
 */
BpTree.prototype.removeFromLeaf = function(leaf, key, path_, cb) {
  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  var self = this
    , path = u.clone(path_) //Q: is this necessary?
    , deadHdls = []

  leaf.remove(key)

//  console.warn("BpTree#removeFromLeaf: removed key=%j; leaf=%s", key, leaf)

  if (this.root === leaf) {
    assert.equal(path.length, 0, "BpTree#removedFromLeaf: path.length != 0")

    if (leaf.size() > 0) {
      this.replaceRoot(leaf, deadHdls, cb)
    }
    else {
      this.replaceRoot(null, deadHdls, cb)
    }

    return
  }

  if ( !leaf.toSmall() ) {
    this.storeLeaf(leaf, path, cb)
    return
  }
  //leaf.toSmall() gotta merge/split/store rinse&repeat

  this.doMerge(leaf, path, function(err, mergedLeaf, pKey, deadLeaf){
    var parent = path.pop()
      //, oldHdl

    deadHdls.push(deadLeaf.hdl)

    if ( !mergedLeaf.toBig() ) {

      //oldHdl = mergedLeaf.hdl

      self.store(mergedLeaf, function(err, newHdl){
        if (err) { cb(err); return }

        self.removeFromBranch( parent, pKey, mergedLeaf, path
                             , deadHdls, cb)
      })
      return
    }
    //mergedLeaf.toBig needs a .split

     var pair = mergedLeaf.split()
       , npKey = pair[0]
       , rLeaf = pair[1]

    var rmHdl = parent.remove(pKey)
    //rmHdl == deadLeaf.hdl; so it is already in dealHdls[]

    self.store(rLeaf, function(err, newHdl){
      if (err) { cb(err); return }

      parent.add(npKey, mergedLeaf.hdl, rLeaf.hdl)

      path.push(parent)

      self.storeSimplePath( mergedLeaf, path, deadHdls, cb)
    }) //.store rLeaf
  }) //.doMerge
} //.removeFromLeaf


/**
 * Remove a child from a Branch by key
 * and propengate changes up the tree if needed.
 *
 * @param {Node} branch node being modified
 * @parem {Key} key key of the column to be removed
 * @param {Node} rNode the replacement node for i+1
 * @param {Array} path
 * @param {Array} deadHdls
 * @param {Function} cb cb(err)
 *
 */
BpTree.prototype.removeFromBranch = function(branch, key, rNode, path, deadHdls, cb){
  assert(branch instanceof Branch, "WTF! this is supposed to be a Branch")

  var self = this

//  console.warn("\nBpTree#removedFromBranch: key=%s; branch=%s; rNode=%s;", key, branch, rNode)

  var rmHdl = branch.remove(key)

  if (this.root === branch) {
    assert.equal(path.length, 0, "BpTree#removedFromBranch: path.length != 0")

    if (branch.size() > 1) {
      this.replaceRoot(branch, deadHdls, cb)
    }
    else {
      deadHdls.push(branch.hdl)
      // branch.children.lenth == 1
      assert.ok(branch.children.length == 1)
      this.replaceRoot(rNode, deadHdls, cb)
    }

    return
  }

  if ( !branch.toSmall() ) {
    this.storeSimplePath(branch, path, deadHdls, cb)
    return
  }
  //branch.toSmall needs .doMerge

  this.doMerge(branch, path, function(err, mergedBranch, pKey, deadBranch){
    var parent = path.pop()
      //, oldHdl

    deadHdls.push(deadBranch.hdl)

    if ( !mergedBranch.toBig() ) {

      //oldHdl = mergedBranch.hdl

      self.store(mergedBranch, function(err, hdl){
        if (err) { cb(err); return }

        self.removeFromBranch(parent, pKey, mergedBranch, path
                             , deadHdls, cb)
      })
      return
    }
    //mergedBranch.toBig needs a .split

    var pair = mergedBranch.split()
      , npKey   = pair[0]
      , rBranch = pair[1]

    var rmHdl = parent.remove(pKey)
    //rmHdl == deadBranch.hdl; so it is already in dealHdls[]

    self.store(rBranch, function(err, hdl){
      if (err) { cb(err); return }

      parent.add(npKey, mergedBranch.hdl, rBranch.hdl)

      path.push(parent)

      self.storeSimplePath( mergedBranch, path, deadHdls, cb)
    }) //.store rBranch
  }) //.doMerge
} //.removedFromBranch


/**
 *
 * @param {Node} node
 * @param {Array} path array of branches leading to root
 * @param {Function} cb cb(err, mergedBranch, pKey, deadBranch)
 */
BpTree.prototype.doMerge = function(node, path, cb){
  var self = this

  //how can you merge something at root?!?
  assert.ok(path.length > 0)
  assert.ok(node !== this.root)

  //find sibling to merge with

  var parent = path[path.length-1] //non-destructive use of path
    , lNode, rNode
    , sibHdl
    , pKey

  //is the current node the left-most child in parent?
  for (var idx=0; idx<parent.children.length; idx += 1)
    if ( node.hdl.equals(parent.children[idx]) ) break

  assert(idx != parent.children.length, "node.hdl not found in parent.children")

  if ( idx == parent.children.length-1 ) { //right-most child
    sibHdl = parent.children[idx-1] //node to the left
    rNode = node
    //lNode == undefined
  }
  else {
    //this should be the common case; idx := [ 0, parent.children.length )
    //sibling is the node to the right
    sibHdl = parent.children[idx+1]
    lNode = node
    //rNode == undefined
  }

  this.load(sibHdl, function(err, sibNode){
    if (err) { cb(err); return }

    if (lNode) { //if lNode is defined; pKeyIdx = idx,  rNodeIdx=idx+1
      //lNode == node
      rNode = sibNode
      pKey = parent.keys[idx] //lNode == node == parent.children[idx]
    }
    else { //lNode is undefined; pKeyIdx = idx-1, rNodeIdx = idx
      //rNode == node
      lNode = sibNode
      pKey = parent.keys[idx-1] // lNode == sibNode == parenth.children[idx-1]
    }

    self.mergeOp(lNode, rNode, function(err){
      if (err) { cb(err); return }

      //(err, mergedBranch, pKey, deadBranch)
      cb(null, lNode, pKey, rNode)
    }) //mergeOp
  })
} //.doMerge

BpTree.prototype.mergeOp = function(lNode, rNode, cb){
  assert.equal(lNode.constructor, rNode.constructor)

  if (lNode instanceof Leaf) {
    lNode.merge(rNode)
    cb(null, lNode, rNode)
    return
  }
  assert(lNode instanceof Branch)
  this.findLeftMostKey(rNode, function(err, lmKey){
    if (err) { cb(err); return }

    lNode.merge(lmKey, rNode)

    cb(null, lNode, rNode)
  })
} //.mergeOp


/**
 * Traverse a node recusively to find the left-most key
 *
 * @param {Node} node
 * @param {Function} cb cb(err, key)
 */
BpTree.prototype.findLeftMostKey = function(node, cb){
  var self = this

  if (node instanceof Leaf) {
    var key = node.keys[0]
    cb(null, key)
    return
  }

  this.load(node.children[0], function(err, n){
    if (err) { cb(err); return }
    self.findLeftMostKey(n, cb)
  })
} //.findLeftMostKey


/**
 *
 * @param {Node} newRoot
 * @param {Array} deadHdls handles no longer used by tree
 * @param {Function} cb cb(err)
 */
BpTree.prototype.replaceRoot = function(newRoot, deadHdls, cb){
  var self = this
    , oldRoot = this.root

//  console.warn("BpTree#replaceRoot: newRoot=%s", newRoot)

  this.root = newRoot

  if (oldRoot && newRoot == null)
    deadHdls.push(oldRoot.hdl)

  if (newRoot) {
    this.store(newRoot, function(err, hdl){
      if (err) { cb(err); return }

//      if ( oldRoot && !oldRoot.hdl.equals(newRoot.hdl) )
//        deadHdls.push(oldRoot.hdl)

      self.cleanHandles(deadHdls, cb)
    })
    return
  }
  //newRoot == null

  this.cleanHandles(deadHdls, cb)
}

/**
 * Clean up handles no longer used by the tree.
 *
 * @param {Array} array of andles no longer in use by the tree
 * @param {Function} cb cb(err)
 */
BpTree.prototype.cleanHandles = function(deadHdls, cb){
  var self = this

//  console.warn("BpTree#cleanhandles: called!")

  async.eachSeries(
    deadHdls
  , function(deadHdl, ecb){
      assert.ok(deadHdl)
      self.storage.release(deadHdl, ecb)
    }
  , function(err, res){
      if (err) { cb(err); return }
      self.storage.flush(cb)
    }) //async.eachSeries
}

/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err, node) node might be Branch xor Leaf type
 */
BpTree.prototype.load = function(hdl, cb){
  var self = this

  this.storage.load(hdl, function(err, n /*, hdl*/){
    if (err) { cb(err); return }

    assert( u.isPlainObject(n)
          , format("BpTree#load: n is not a plain object; n=%j", n) )

    for (var i=0; i<n.children.length; i++)
      assert.ok(typeof n.children[i] != 'undefined')

    var node
    if ( n.type == 'Leaf') {
      node = new Leaf(n.order, n.keys, n.children, self.keyCmp)
    }
    else if ( n.type == 'Branch') {
      assert.equal(
        n.keys.length+1
      , n.children.length
      , format("n.keys.length+1 != n.children.length; hdl=%j; n = %j", hdl, n))

        var children = n.children
                       .map(function(c){ return self.storage.handleFromJSON(c) })

      assert.equal(n.children.length, children.length)

      node = new Branch(n.order, n.keys, children, self.keyCmp, true)
    }
    else throw new Error("unknown n.type = "+n.type)

    node.hdl = hdl
    cb(err, node)
  })

} //.load()


/**
 * Store a node
 *
 * @param {Node} node
 * @param {Function} cb cb(err, hdl)
 */
BpTree.prototype.store = function(node, cb){
  var self = this

  if (this.writing) {
    console.error("*** this.writing == true; node = %s", node)
    cb(new Error("BpTree.prototype.store: store already in progress."))
  }

  this.writing = true
//  console.warn("BpTree#store: set writing=%j", this.writing)
//  console.warn("BpTree#store: STORING node=%s", node)

  var n = { "type"     : ""
          , "order"    : node.order
          , "keys"     : u.clone(node.keys, true /*deep*/)
          , "children" : []
          }

  if      (node instanceof Leaf  ) {
    assert.equal(node.keys.length, node.children.length) //TMP
    n.type = "Leaf"
    n.children = u.cloneDeep(node.children)
  }
  else if (node instanceof Branch) {
    assert.equal(
      node.keys.length+1, node.children.length
    , format("node.keys.length+1 != node.children.length; node = %s", node) )

    n.type = "Branch"
    n.children = node.children
                 .map(function(hdl){ return self.storage.handleToJSON(hdl) })
  }
  else {
    console.error("BpTree#store: unknown node type: node = %j", node)
    throw new Error("Unknown node type")
  }

  for (var i=0; i<n.children.length; i++)
    assert.ok(typeof n.children[i] != 'undefined')

  //for a fresh new node (leaf or branch) node.hdl will be null
  this.storage.store(n, node.hdl, function(err, hdl){
    self.writing = false
//    console.warn("BpTree#store: set writing=%j", self.writing)
//    console.warn("BpTree#store: cb: STORED: hdl=%s; n=%j", hdl, n)

    if (err) { cb(err); return }

    assert.ok(hdl != null, 'hdl == null')

    node.hdl = hdl //this is done here AUTHORITATIVE!

    node.dirty = false //NOTE: only matters for Branch nodes

    cb(null, hdl)
  })

} //.store()


/**
 *
 * @param {Function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//