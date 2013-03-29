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
  , Dequeue = require('dequeue')
  , Handle = require('block-file').Handle
  , Node = require('./node')
  , Branch = require('./branch')
  , Leaf = require('./leaf')

/**
 * B+Tree data struture
 *
 * @param {number} order
 * @param {function} keyType
 */
exports = module.exports = BpTree
function BpTree(order /*, keyType, idType*/) {
  assert(typeof order === 'number', "order must be an number")
  assert(order%1===0, "order must be an integer")
  assert(order > 0, "order must be positive")
//  assert.equal(typeof keyType, 'function', "keyType must be a function")

//  this.keyType = keyType
//  this.idType = idType
  this.order = order
  this.root = null
  this.writing = false
  this.cache = new Cache()
} //constructor

BpTree.Node = Node
BpTree.Leaf = Leaf
BpTree.Branch = Branch
BpTree.Handle = Handle

/**
 * Cache constructor
 */
function Cache(max){
  this.max = max || 1000
  this.nodeQ = new Dequeue()
  this.nodes = []
}

Cache.prototype.size = function(){
  return this.nodeQ.length
}

Cache.prototype.put = function(hdl, node){
  this.nodeQ.push(hdl)
  this.nodes[hdl] = node

  var victimHdl
  if (this.size() > this.max) {
    victimHdl = this.nodeQ.shift()
    delete this.nodes[victimHdl]
  }
  return this
}

Cache.prototype.get = function(hdl){
  return this.nodes[hdl]
}


/**
 * Insert a Key-Value pair in the B+Tree
 *
 * @param {Key} key
 * @param {object} value
 * @param {function} cb cb(err, [...ids])
 */
BpTree.prototype.put = function(key, value, cb){
  assert(!u.isUndefined(value), "BpTree#put: value of undefined is not allowed")
  var self = this
  if (this.root === null) { //empty tree
    //create leaf node
    this.root = new Leaf(this.order, [key], [value])
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
 * @param {function} cb cb(err, data)
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
 * @param {function} cb cb(err, data)
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

    self.removeFromLeaf(leaf, path, key, function(err, freedIds, newIds){
      if (err) { cb(err); return }
      //do something with freedIds & newIds
      cb(null, data)
    })
  })
} //.del()


/**
 * Visit each node pre-order traversal.
 *
 * @param {function} visit visit(node, isLeaf, depth)
 * @param {function} [done] done(err, res)
 */
BpTree.prototype.traversePreOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodePreOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.parallel(
        node.children.map(function(id){
          return function(pcb){ self.load(id, pcb) }
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
        }) //async.parallel id
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
 * @param {function} visit visit(node, isLeaf, depth)
 * @param {function} [done] done(err, res)
 */
BpTree.prototype.traverseInOrder = function(visit, done){
  var self = this
    , res = []

  function traverseNodeInOrder(node, depth, nodeDone){
    if (node instanceof Branch) {
      res.push( visit(node, false, depth) )
      async.series(
        node.children.map(function(id, i){
          return function(next){
            self.load(id, function(err, child){
              traverseNodeInOrder(child, depth+1, next) })
          }
        })
      , nodeDone) //async.series id
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
 * @param {function} visit visit(key, data)
 * @param {function} [done] done(err, res)
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
 * @param {function} cb cb(err, leaf, path) path isa Array of Branch objects
 */
BpTree.prototype.findLeaf = function(key, cb){
  if (this.root === null) { cb(null, null); return }

  var self = this
    , path = []

  function find(err, node) {
    if (err) { cb(err); return }
    if (node instanceof Branch) {
      //node isa Branch so node.children contains IDs
      path.push(node)
      for (var i=0; i<node.keys.length; i++)
        if ( key.cmp(node.keys[i]) < 0 ) break
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
 * @param {function} cb cb(err, [...newIds])
 */
BpTree.prototype.insert = function(node, path_, key, child, cb) {
  var self = this
    , path = u.clone(path_)
    , newIds = []

  node.add(key, child)
  if ( !node.toBig() ) {
    //This is non-COW version
    this.store(node, function(err, id){
      if (err) { cb(err); return }
      cb(null, newIds)
    })
    return
  }
  //node.toBig() needs node.split()

  var pair = node.split()
    , pKey = pair[0] //pKey parent key
    , sibl = pair[1] //nNode next sibling node to "node"

  var oNodeId, nNodeId

  async.series(
    [ function(scb){
        self.store(node, scb)
      }
    , function(scb){
        self.store(sibl, scb) //first time sibl.id gets set
      }
    , function(scb){
        //if (node === self.root)
        if (path.length == 0) {
          self.root = new Branch(self.order, [pKey], [node.id, sibl.id])
          path.unshift(self.root)
          self.store(self.root, scb)
        }
        else {
          var parent = path.pop()
          self.insert(parent, path, pKey, sibl.id, scb)
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
 * @param {function} cb cb(err, [...freedIds], [...newIds])
 *
 */
BpTree.prototype.removeFromLeaf = function(leaf, path_, key, cb) {
  var self = this
    , path = u.clone(path_)
    , pair , idx, data
    , reverse, parent
    , freedIds = [], newIds = []

  assert(leaf instanceof Leaf, "WTF! this is supposed to be a Leaf")

  pair = leaf.remove(key)
  if (u.isUndefined(pair)) {
    cb(new Error("leaf.remove(key) failed"))
    return
  }

  function noopStored(err, id){
    if (err) { cb(err); return }
    cb(null, freedIds, newIds)
  }

  if (leaf === this.root) {
    assert.strictEqual(path.length, 0)

    if (leaf.size() > 0) {
      ///Leaf root leafs MAY have only one key-value pair aka size()>0
      this.store(leaf, noopStored)
    }
    else {
      freedIds.push(this.root.id)
      this.root = null
      cb(null, freedIds)
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

  //who am i; which child(idx) of parent is leaf.id
  var leafIdx, siblIdx //leaf and sibl id indexes in parent
  for (leafIdx=0; leafIdx < parent.children.length; leafIdx++)
    if ( leaf.id.equals(parent.children[leafIdx]) ) break

  assert(leafIdx != parent.children.length, "did not find leaf.id in parent.children")

  if (leafIdx == parent.children.length-1) {
    //if leaf.id is the last child of parent
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
          freedIds.push(mergedRLeaf.id) //which ever is mergedRLeaf is freed

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
          self.store(splitRLeaf, function(err, id){
            if (err) { scb(err); return }
            newIds.push(id)
            scb(null)
          })
        }
        else scb(null)
      }
    , function(scb){
        self.store(lLeaf, function(err, id){
          if (err) { scb(err); return }
          scb(null)
        })
      }
    , function(scb){
        if (splitRLeaf) {
          parent.removeById(mergedRLeaf.id)
          parent.add(splitRLeafPKey, splitRLeaf.id)
          scb(null)
        }
        else {
          //if ( key.equals(lLeafPKey) )

          self.removeFromBranch(parent, path, mergedRLeaf.id, function(err, fIds, nIds){
            if (err) { scb(err); return }
            freedIds.push.apply(freedIds, fIds)
            newIds.push.apply(newIds, nIds)
            scb(null)
          })
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      cb(null, freedIds, newIds)
    }
  ) //async.series
} //.removeFromLeaf()


/**
 * Remove a child from a Branch by Id
 * and propengate changes up the tree if needed.
 *
 * @param {Node} parent
 * @param {Array(Branch)} path
 * @param {Id} id
 * @param {function} cb cb(err, [...freedIds], [...newIds])
 *
 */
BpTree.prototype.removeFromBranch = function(node, path_, id, cb) {
  var self = this
    , path = u.clone(path_)
    , pair , idx, key
    , reverse, parent
    , freedIds = [], newIds = []

  assert.ok(node instanceof Branch)

  pair = node.removeById(id)
  if (u.isUndefined(pair)) {
    cb(new Error("node.remove(id) failed"))
    return
  }

  idx = pair[0]
  key = pair[1]

  freedIds.push(id)

  function noopStored(err, id){
    if (err) { cb(err); return }
    cb(null, freedIds, newIds)
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
        freedIds.push(this.root.id)
        self.root = node
        cb(null, freedIds)
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

  //who am i; which child(idx) of parent is node.id
  var nodeIdx, siblIdx //node and sibl id indexes in parent
  for (nodeIdx=0; nodeIdx < parent.children.length; nodeIdx++)
    if ( node.id.equals(parent.children[nodeIdx]) ) break

  assert(nodeIdx != parent.children.length, "did not find node.id in parent.children")

  if (nodeIdx == parent.children.length-1) {
    //if node.id is the last child of parent
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
            freedIds.push(mergedRNode.id) //which ever is mergedRNode is freed


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

          self.store(splitRNode, function(err, id){
            if (err) { scb(err); return }
            newIds.push(id)
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
        self.store(lNode, function(err, id){
          if (err) { scb(err); return }
          scb(null)
        })
      }
    , function(scb){
        if (splitRNode) {
          parent.removeById(mergedRNode.id)
          parent.add(splitRNodePKey, splitRNode.id)
          scb(null)
        }
        else {
          //if ( key.equals(mergedRNodePKey) )

          self.removeFromBranch(parent, path, mergedRNode.id, function(err, fIds, nIds){
            if (err) { scb(err); return }
            freedIds.push.apply(freedIds, fIds)
            newIds.push.apply(newIds, nIds)
            scb(null)
          })
        }
      }
    ]
  , function(err, res){
      if (err) { cb(err); return }
      cb(null, freedIds, newIds)
    }
  ) //async.series
} //.removeFromBranch()


/**
 * Load a node
 *
 * @param {Handle} hdl
 * @param {function} cb cb(err, node)
 */
var STORE = {}
  , curSegNum = 0
  , curBlkNum = 0

function nextHandle() {
  if (curBlkNum >= Handle.MAX_BLOCKNUM) {
    curBlkNum = 0
    curSegNum += 1
  }
  if (curSegNum > Handle.MAX_SEGNUM)
    throw new Error("ran out of Handle namespace")

  return new Handle(curSegNum, curBlkNum++, 0)
}

BpTree.prototype.load = function(hdl, cb){
  var self = this
    , node

  assert(hdl instanceof Handle, "hdl not instanceof Handle")

  node = this.cache.get(hdl)
  if (node) {
    //process.nextTick(function(){ cb(null, node) })
    setImmediate(function(){ cb(null, node) })
    return
  }

  function loaded(err, node) {
    //if it is an Inner node add it to the cache
    if (node instanceof Node) self.cache.put(hdl, node)
    cb(err, node)
  }

  setTimeout(function(){
    var node = STORE[hdl]
      , err=null
    if (!node) {
      err = new Error(format("hdl not it STORE; hdl=%s", hdl))
      err.code = "ENOEXIST"
    }
    loaded(err, node)
  }, 10)
} //.load()


/**
 * Store a node
 *
 * @param {Node} node
 * @param {function} cb cb(err, id)
 */
BpTree.prototype.store = function(node, cb){
  var self = this
    , hdl

  if (this.writing) {
    throw new Error("store already in progress.")
  }

  this.writing = true

  //COW always generates a new Handle for every write
  //node.id = nextHandle()

  //This is non-COW version
  if (node.id === null) {
    node.id = nextHandle()
  }


  function stored(err, id) {
    if (err) { cb(err); return }
    self.writing = false
    cb(null, id)
  }

  if (node instanceof Branch) self.cache.put(node.id, node)

  setTimeout(function(){
    STORE[node.id] = node
    stored(null, node.id)
  }, 100)

  return node.id
} //.store()


/**
 * Store a node and optional sibling. along with the path. (COW semantics)
 *
 * @param {Node} node
 * @param {Node|null} sib
 * @param {Array(Node)} path path is root..parent
 * @param {function} cb cb(err, nodeId, pathIds)
 */
BpTree.prototype.storePath = function(node, path_, cb){
  var self = this
    , path = u.clone(path_)

  //path[0] == root
  if (path.length > 0) assert.strictEqual(this.root, path[0])

  path.push(node)
  path.reverse() //path now goes upto root

  var oldIds = []

  async.series(
    path.map(function(n, i, a){ //n == node; i == index; a == path
      return function(scb){
        var oldId = n.id
          , newId = self.store(n, scb)
        oldIds.push(oldId)
        if (i+1 < a.length) //parent exists
          a[i+1].setChild(oldId, newId) //a[i+1] == parent
      }
    })
  , function(err, pathIds){
      if (err) { cb(err); return }

      pathIds.reverse()
      oldIds.reverse()

      //self.rootId = pathIds[0]
      var nodeId = pathIds.pop()

      cb(null, nodeId, pathIds, oldIds)
    })

} //.storePath()


/**
 *
 * @param {function} cb cb(err)
 */
BpTree.prototype.save = function(cb){
  throw new Error("not implemented")
} //.save()


//