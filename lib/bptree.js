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
  assert(order > 0, "order must be positive")

  this.order = order
  if (keyCmp == null || keyCmp == 'string')
    this.keyCmp = strCmp
  else
    this.keyCmp = keyCmp

  if (typeof storage == 'undefined')
    this.storage = new TrivialStore(true)
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
 * @param {Function} cb cb(err, [...hdls])
 */
BpTree.prototype.put = function(key, value, cb){
  assert(!u.isUndefined(value), "BpTree#put: value of undefined is not allowed")
  var self = this
  if (this.root === null) { //empty tree
    //create leaf node
    this.root = new Leaf(this.order, [key], [value], this.keyCmp)
    this.store(this.root, cb)
    return
  }

  this.findLeaf(key, function(err, leaf, path){
    if (err) { cb(err); return }

    self.insert(leaf, path, key, value, cb)
  })
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
      cb(null, undefined) //undefined means not found
      return
    }

    self.removeFromLeaf(leaf, path, key, function(err, freedHdls, newHdls){
      if (err) { cb(err); return }
      //do something with freedHdls & newHdls
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

      if (typeof node.children[i] == 'undefined') {
        console.warn("BpTree#findLeaf: find: node.children = %j", node.children)
        throw new Error(format("BpTree#findLeaf: find: node.children[%d] == undefined; node.keys = %j; node.children = %j",i, node.keys, node.children))
      }

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
 * @param {Function} cb cb(err, [...newHdls])
 */
BpTree.prototype.insert = function(node, path_, key, child, cb) {
  var self = this
    , path = u.clone(path_)
    , newHdls = []

  node.add(key, child)
  if ( !node.toBig() ) {
    //This is non-COW version
    this.store(node, function(err, hdl){
      if (err) { cb(err); return }
      node.hdl = hdl
      cb(null, newHdls)
    })
    return
  }
  //node.toBig() needs node.split()

  var pair = node.split()
    , pKey = pair[0] //pKey parent key
    , sibl = pair[1] //nNode next sibling node to "node"

  var oNodeHdl, nNodeHdl

  async.series(
    [ function(scb){
        self.store(node, scb)
      }
    , function(scb){
        self.store(sibl, scb) //first time sibl.hdl gets set
      }
    , function(scb){
        //if (node === self.root)
        if (path.length == 0) {
          self.root = new Branch(self.order, [pKey], [node.hdl, sibl.hdl], self.keyCmp)
          path.unshift(self.root)
          self.store(self.root, scb)
        }
        else {
          var parent = path.pop()
          self.insert(parent, path, pKey, sibl.hdl, scb)
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      cb(null, u.flatten(res))
    }
  )
} //.insert()


/**
 * Remove a child from a Leaf by Key
 * and propengate changes up the tree if needed.
 *
 * @param {Node} parent
 * @param {Array(Branch)} path
 * @param {Key} key
 * @param {Function} cb cb(err, [...freedHdls], [...newHdls])
 *
 */
BpTree.prototype.removeFromLeaf = function(leaf, path_, key, cb) {
  var self = this
    , path = u.clone(path_)
    , data
    , reverse, parent
    , freedHdls = [], newHdls = []

  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  data = leaf.remove(key)
  if (u.isUndefined(data)) {
    cb(new Error("leaf.remove(key) failed"))
    return
  }

  function noopStored(err, hdl){
    if (err) { cb(err); return }
    cb(null, freedHdls, newHdls)
  }

  if (leaf === this.root) {
    assert.strictEqual(path.length, 0)

    if (leaf.size() > 0) {
      ///Leaf root leafs MAY have only one key-value pair aka size()>0
      this.store(leaf, noopStored)
    }
    else {
      freedHdls.push(this.root.hdl)
      this.root = null
      cb(null, freedHdls)
    }

    return
  }
  if ( !leaf.toSmall() ) {
    //This is non-COW version
    this.store(leaf, noopStored)
    return
  }
  //leaf.toSmall() gotta merge/split/store rinse&repeat

  reverse = false
  parent = path.pop()

  //who am i; which child(idx) of parent is leaf.hdl
  var leafIdx, siblIdx //leaf and sibl hdl indexes in parent
  for (leafIdx=0; leafIdx < parent.children.length; leafIdx++)
    if ( u.isEqual( leaf.hdl, parent.children[leafIdx] ) ) break

  assert(leafIdx != parent.children.length, "did not find leaf.hdl in parent.children")

  if (leafIdx == parent.children.length-1) {
    //if leaf.hdl is the last child of parent
    // pick the previous sibling of leaf and reverse lleaf and rleaf for merge.
    // reverse cuz we always merge right
    //this is ok cuz parent.children.length >= 2 under all valid values of order
    reverse = true
    siblIdx = leafIdx - 1
  }
  else {
    //else pick the next(+1) sibling of leaf; we merge right
    siblIdx = leafIdx + 1
  }

  var lLeaf
    , mergedRLeaf, mergedRLeafIdx, mergedRLeafPKey
    , splitRLeaf = null, splitRLeafPKey

  async.series(
    [ //Load sibling leaf and set the lLeaf and mergedRLeaf
      //then do the merge.
      function (scb) {
        self.load(parent.children[siblIdx], function(err, sibl){
          if (reverse) {
            lLeaf = sibl
            mergedRLeaf = leaf
            mergedRLeafIdx = leafIdx
          }
          else {
            lLeaf = leaf
            mergedRLeaf = sibl
            mergedRLeafIdx = siblIdx
          }

          lLeaf.merge(mergedRLeaf)
          freedHdls.push(mergedRLeaf.hdl) //which ever is mergedRLeaf is freed

          mergedRLeafPKey = leaf.keys[0]
          scb(null)

        })
      }
      //Check if the lLeaf needs to split.
      //If lLeaf is toBig then split it. The
      // new split leaf and we must find its left-most-key
      //Else do nothing
    , function(scb){
        var pair, nKey
        if (lLeaf.toBig()) {
          pair  = lLeaf.split()
          nKey  = pair[0]
          splitRLeaf = pair[1]

          splitRLeafPKey = splitRLeaf.keys[0]
          self.store(splitRLeaf, function(err, hdl){
            if (err) { scb(err); return }
            newHdls.push(hdl)
            scb(null)
          })
        }
        else scb(null)
      }
    , function(scb){
        self.store(lLeaf, function(err, hdl){
          if (err) { scb(err); return }
          scb(null)
        })
      }
    , function(scb){
        if (splitRLeaf) {
          parent.removeByHdl(mergedRLeaf.hdl)
          parent.add(splitRLeafPKey, splitRLeaf.hdl)
          scb(null)
        }
        else {
          //if ( u.isEqual(key, lLeafPKey) )

          self.removeFromBranch(parent, path, mergedRLeaf.hdl, function(err, fHdls, nHdls){
            if (err) { scb(err); return }
            freedHdls.push.apply(freedHdls, fHdls)
            newHdls.push.apply(newHdls, nHdls)
            scb(null)
          })
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      cb(null, freedHdls, newHdls)
    }
  ) //async.series
} //.removeFromLeaf()


/**
 * Remove a child from a Branch by Hdl
 * and propengate changes up the tree if needed.
 *
 * @param {Node} parent
 * @param {Array(Branch)} path
 * @param {Hdl} hdl
 * @param {Function} cb cb(err, [...freedHdls], [...newHdls])
 *
 */
BpTree.prototype.removeFromBranch = function(node, path_, hdl, cb) {
  var self = this
    , path = u.clone(path_)
    , key
    , reverse, parent
    , freedHdls = [], newHdls = []

  assert.ok(node instanceof Branch)

  key = node.removeByHdl(hdl)
  if (u.isUndefined(key)) {
    cb(new Error("node.remove(hdl) failed"))
    return
  }

  freedHdls.push(hdl)

  function noopStored(err, hdl){
    if (err) { cb(err); return }
    cb(null, freedHdls, newHdls)
  }

  if (node === this.root) {
    assert.strictEqual(path.length, 0)
    if (node.size() > 1) {
      //Branch root nodes MUST have atleast two children aka size()>1
      this.store(node, noopStored)
    }
    else {
      assert.strictEqual(this.root.children.length, 1)
      //promote the one remaining child to root
      this.load(this.root.children[0], function(err, node){
        if (err) { cb(err); return }
        freedHdls.push(this.root.hdl)
        self.root = node
        cb(null, freedHdls)
      })
    }
    return
  }
  if ( !node.toSmall() ) {
    this.store(node, noopStored)
    return
  }
  //node.toSmall() gotta merge/split/store rinse&repeat

  reverse = false
  parent = path.pop()

  //who am i; which child(idx) of parent is node.hdl
  var nodeIdx, siblIdx //node and sibl hdl indexes in parent
  for (nodeIdx=0; nodeIdx < parent.children.length; nodeIdx++)
    if ( u.isEqual( node.hdl, parent.children[nodeIdx] ) ) break

  assert(nodeIdx != parent.children.length, "did not find node.hdl in parent.children")

  if (nodeIdx == parent.children.length-1) {
    //if node.hdl is the last child of parent
    // pick the previous sibling of node and reverse lnode and rnode for merge.
    // reverse cuz we always merge right
    //this is ok cuz parent.children.length >= 2 under all valid values of order
    reverse = true
    siblIdx = nodeIdx - 1
  }
  else {
    //else pick the next(+1) sibling of node; we merge right
    siblIdx = nodeIdx + 1
  }

  function findLeftMostKey(node, cb){
    if (node instanceof Leaf) {
      cb(null, node.keys[0])
      return
    }
    //node isa Branch
    self.load(node.children[0], function(err, n){
      if (err) { cb(err); return }
      findLeftMostKey(n, cb)
    })
  }

  var lNode
    , mergedRNode, mergedRNodeIdx, mergedRNodePKey
    , splitRNode = null, splitRNodePKey

  async.series(
    [ //Load sibling node and set the lNode and mergedRNode
      //then do the merge.
      function (scb) {
        self.load(parent.children[siblIdx], function(err, sibl){
          if (reverse) {
            lNode = sibl
            mergedRNode = node
            mergedRNodeIdx = nodeIdx
          }
          else {
            lNode = node
            mergedRNode = sibl
            mergedRNodeIdx = siblIdx
          }

          findLeftMostKey(mergedRNode, function(err, k){
            if (err) { scb(err); return }
            mergedRNodePKey = k

            mergedRNode.keys.unshift(mergedRNodePKey)

            lNode.merge(mergedRNode)
            freedHdls.push(mergedRNode.hdl) //which ever is mergedRNode is freed


            scb(null)
          })
        })
      }
      //Check if the lNode needs to split.
      //If lNode is toBig then split it. The
      // new split node and we must find its left-most-key
      //Else do nothing
    , function(scb){
        var pair, nKey
        if (lNode.toBig()) {
          pair  = lNode.split()
          nKey  = pair[0]
          splitRNode = pair[1]

          self.store(splitRNode, function(err, hdl){
            if (err) { scb(err); return }
            newHdls.push(hdl)
            findLeftMostKey(splitRNode, function(err, k){
              if (err) { scb(err); return }
              splitRNodePKey = k
              scb(null)
            })
          })
        }
        else scb(null)
      }
    , function(scb){
        self.store(lNode, function(err, hdl){
          if (err) { scb(err); return }
          scb(null)
        })
      }
    , function(scb){
        if (splitRNode) {
          parent.removeByHdl(mergedRNode.hdl)
          parent.add(splitRNodePKey, splitRNode.hdl)
          scb(null)
        }
        else {
          //if ( u.isEqual(key, mergedRNodePKey) )

          self.removeFromBranch(parent, path, mergedRNode.hdl, function(err, fHdls, nHdls){
            if (err) { scb(err); return }
            freedHdls.push.apply(freedHdls, fHdls)
            newHdls.push.apply(newHdls, nHdls)
            scb(null)
          })
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      cb(null, freedHdls, newHdls)
    }
  ) //async.series
} //.removeFromBranch()


/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {Function} cb cb(err, node) node might be Branch xor Leaf type
 */
BpTree.prototype.load = function(hdl, cb){
  var self = this

  this.storage.load(hdl, function(err, n){
    if (err) { cb(err); return }

    assert( u.isPlainObject(n), "BpTree#load: n is not a plain object")

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

      var Handle = self.storage.constructor.Handle
        , children = n.children.map(function(c){ return Handle.fromJSON(c) })

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
    console.warn("node = %s", node)
    cb(new Error("BpTree.prototype.store: store already in progress."))
  }

  this.writing = true


  var n = {
    "type"     : ""
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

    var Handle = this.storage.constructor.Handle
    n.type = "Branch"
    n.children = node.children.map(function(c){ return Handle.toJSON(c) })

    assert.equal(
      n.children.length
    , node.children.length
    , format("n.children.length != node.children.length; %d != %d"
            , n.children.length, node.children.length) )
    assert.equal(
      n.keys.length+1
    , n.children.length
    , format("n.keys.length+1 != n.children.length; n = %j",n) )

  }
  else throw new Error("Unknown node type")

//  if ((node.hdl != null) && (node.hdl.id == 20))
//    console.warn("BpTree#store: node = %j", node)

  for (var i=0; i<n.children.length; i++)
    assert.ok(typeof n.children[i] != 'undefined')

  //for a fresh new node (leaf or branch) node.hdl will be null
  this.storage.store(n, node.hdl, function(err, hdl){
    self.writing = false
    if (err) { cb(err); return }

    assert.ok(hdl != null, 'hdl == null')

    node.hdl = hdl //new handle is not allowed for now; needs storePath()

    cb(null, node.hdl)
  })

} //.store()


/**
 * Store a node and optional sibling. along with the path. (COW semantics)
 *
 * @param {Node} node
 * @param {Node|null} sib
 * @param {Array(Node)} path path is root..parent
 * @param {Function} cb cb(err, nodeHdl, pathHdls)
 */
BpTree.prototype.storePath = function(node, path_, cb){
  var self = this
    , path = u.clone(path_)

  //path[0] == root
  if (path.length > 0) assert.strictEqual(this.root, path[0])

  path.push(node)
  path.reverse() //path now goes upto root

  var oldHdls = []

  async.series(
    path.map(function(n, i, a){ //n == node; i == index; a == path
      return function(scb){
        var oldHdl = n.hdl
        self.store(n, function(err, newHdl){
          if (err) { scb(err); return }
          oldHdls.push(oldHdl)
          if (i+1 < a.length) //parent exists
            a[i+1].setChild(oldHdl, newHdl) //a[i+1] == parent
          scb(null, newHdl)
        })
      }
    })
  , function(err, newHdls){
      if (err) { cb(err); return }

      newHdls.reverse()
      oldHdls.reverse()

      //self.rootHdl = newHdls[0]
      var nodeHdl = newHdls.pop()

      cb(null, nodeHdl, newHdls, oldHdls)
    })

} //.storePath()


/**
 *
 * @param {Function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//