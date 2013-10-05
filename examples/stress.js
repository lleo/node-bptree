#!/usr/bin/env node

var assert = require('assert')
  , util = require('util')
  , format = util.format
  , u = require('lodash')
  , BpTree = require('..')
  , Node = BpTree.Node
  , Leaf = BpTree.Leaf
  , Branch = BpTree.Branch
  , Handle = BpTree.Handle
  , Key = BpTree.Key
  , async = require('async')
  , log = console.log
  , node
  , randOps = require('../lib/rand_ops')
  , rand = randOps.rand
  , randInt = randOps.randInt
  , randomize = randOps.randomize
  , strOps = require('../lib/str_ops')
  , strCmp = strOps.cmp
  , incStr = strOps.inc
  , multStr = strOps.repeat
  , TrivialStore = require('../lib/trivial_store')
  , MemStore = require('../lib/mem_store')
  , BlockFileStore = require('../lib/block_file_store')
  , BlockFile = require('block-file')
  , cmdLine = require('commander')
  , Stats = require('stats-api')
  , perm_fn = 'stress.bf'
  , fs = require('fs')

var keyOrder = function(s){
  if (s == "rev" || s == "reverse")
    return "reverse"
  if (s == "rand" || s == "random" || s == "randomize")
    return "randomize"
  return "inorder"
}

var parseStore = function(s){
  if (s == 'triv' || s == 'trivial')
    return 'TrivialStore'
  if (s == 'mem' || s == 'memory')
    return 'MemStore'
  if (s == 'block-file' || s == 'file')
  return 'BlockFileStore'
}

cmdLine
.version('0.0.1')
.option('-o|--order <n>', 'order of the BpTree', parseInt, 3)
.option('-n|--number <n>', 'Number of Key/Val pairs', parseInt, 10)
.option('-d|--deletion [key-order]', "Order of deletion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-i|--insertion [key-order]', "Order of insertion: inorder, (rev)erse, (rand)omize", keyOrder, 'inorder')
.option('-D|--io-delay <ms>', "Delay each delete (milliseconds)", parseInt, -1)
.option('-S|--use-store <store>', "IO Store to use '(mem)ory', '(triv)ial', block-(file)", parseStore, 'TrivialStore')
.option('--show-foreach', "show each k,v pair via .forEach")
.option('--no-delete-keys', "Don't delete the keys")
.option('--display-put', "Display each tree.put() call")
.option('--display-del', "Display each tree.del() call")
.option('--display-modified', "Display the tree after each delete")
.option('--no-display-final', "Don't display the final tree")
.option('--stats', "Show all stats")
.parse(process.argv)

var order = cmdLine.order
  , numEnts = cmdLine.number
  , insOrder = cmdLine.insertion
  , delOrder = cmdLine.deletion
  , ioDelay = cmdLine.ioDelay
  , useStore = cmdLine.useStore
  , showForeach = cmdLine.showForeach ? true : false
  , deleteKeys = cmdLine.deleteKeys ? true : false
  , displayPut = cmdLine.displayPut ? true : false
  , displayDel = cmdLine.displayDel ? true : false
  , displayModified = cmdLine.displayModified ? true : false
  , displayFinal = cmdLine.displayFinal ? true : false
  , showStats = cmdLine.stats ? true : false

console.log("order             = %d", order)
console.log("number            = %d", numEnts)
console.log("insertion         = %s", insOrder)
console.log("deletion          = %s", delOrder)
console.log("io-delay          = %d", ioDelay)
console.log("use-store         = %s", useStore)
console.log("show-foreach      = %j", showForeach)
console.log("delete-keys       = %j", deleteKeys)
console.log("display-put       = %j", displayPut)
console.log("display-del       = %j", displayDel)
console.log("display-modified  = %j", displayModified)
console.log("display-final     = %j", displayFinal)
console.log("stats             = %s", showStats)
//process.exit(0)

var displayNodeConcise = (
  function(){
    var cnt=0
    function nodeToStringConcise(node, isLeaf){
      var type = isLeaf ? "Leaf" : "Branch"
      return [ type, "(", node.hdl.toString()
             , ", min=", node.min
             , ", max=", node.max
             , ", #keys=", node.keys.length
             , ", #children=", node.children.length
             ].join('')
    }

    return function(node, isLeaf, depth){
      var indent = depth>0 ? multStr("  ", depth) : ""
      console.log("%s%s", indent, nodeToStringConcise(node, isLeaf))
      return cnt++
    }
  }
)()

var displayNodeRegular = (
  function(){
    var cnt=0
    return function(node, isLeaf, depth){
      var indent = depth>0 ? multStr("  ", depth) : ""
      console.log("%s%s", indent, node)
      return cnt++
    }
  }
)()

var displayNode = order>5 ? displayNodeConcise : displayNodeRegular

var store
  , tree
  , tKey = ""
  , keys = []
  , kv = {}
  , delKeys, insKeys

if (useStore == 'BlockFileStore' && fs.existsSync(perm_fn))
  fs.unlinkSync(perm_fn)

for (var ki = 0; ki<numEnts; ki++){
  keys.push( tKey = incStr(tKey) )
  kv[tKey] = ki
}


insKeys = u.clone(keys)
if (insOrder == "randomize")
  randomize(insKeys)
else if (insOrder == "reverse")
  insKeys.reverse()

delKeys = u.clone(keys)
if (delOrder == "randomize")
  randomize(delKeys)
else if (delOrder == "reverse")
  delKeys.reverse()

function createBpTree(scb){
  var store
  if (useStore == 'MemStore') {
    store = new MemStore(ioDelay)
    tree = new BpTree(strOps.cmp, store, { order: order })
    scb()
  }
  else if (useStore == 'TrivialStore') {
    store = new TrivialStore(ioDelay)
    tree = new BpTree(strOps.cmp, store, { order: order })
    scb()
  }
  else if (useStore == 'BlockFileStore') {
    BlockFile.open(perm_fn, function(err, bf){
      if (err) { scb(err); return }
      store = new BlockFileStore(bf)
      tree = new BpTree(strOps.cmp, store, { order: order })
      scb()
    })
  }
  else throw new Error("WTF! useStore="+useStore)
}

function buildTree(scb){
  console.log("")
  console.log("--- Build Tree: %d tree.put() calls", keys.length)
  async.eachSeries(
    insKeys
  , function(k, ecb){
      if (displayPut) process.stdout.write(format("tree.put( %s, %d )", k, kv[k]))
      tree.put(k, kv[k], function(err){
        if (err) {
          if (displayPut) process.stdout.write(" failed.\n")
          console.error(err)
          ecb(err)
          return
        }
        if (displayPut) process.stdout.write(" success\n")
        ecb()
      })
    }
  , function(err){
      if (err) { scb(err); return }
      console.log("")
      console.log("--- Initial Tree ---")
      tree.traverseInOrder(displayNode, scb)
    })
}

function delTree(scb){
  console.log("")
  console.log("--- Deleting Keys ---")
//  console.log("delKeys=%j", delKeys)

  async.eachSeries(
    delKeys
  , function(k, ecb){
      if (displayDel) process.stdout.write(format("tree.del(%s)", k))

      tree.del(k, function(err){
        if (err) {
          if (displayDel) process.stdout.write(" failed.\n")
          console.error(err)
          ecb(err)
          return
        }
        if (displayDel) process.stdout.write(" success.\n")
        if (displayModified) {
          console.log("--- Modified Tree ---")
          if (tree.root == null) {
            console.log("tree.root == null")
            ecb()
          }
          else
            tree.traverseInOrder(displayNode, ecb)
        }
        else
          ecb()
      })
    }
//  , scb)
  , function(err){
      if (err) { scb(err); return }
//      console.log("END async.eachSeries(delKeys, ...)")
      scb()
    })
}

function displayFinalTree(scb){
  console.log("")
  console.log("--- FINAL TREE ---")
  if (tree.root == null) {
    console.log("tree.root == null")
    scb()
  }
  else
    tree.traverseInOrder(displayNode, scb)
}

function executeForeach(scb){
  console.log("")
  console.log("--- FOREACH(key, val) ---")
  tree.forEach(function(k,v){
    console.log("k = %s; v = %s;", k, v)
  }, scb)
}

var steps = []

steps.push(createBpTree)
steps.push(buildTree)
if (deleteKeys) {
  steps.push(delTree)
}
if (displayFinal) steps.push(displayFinalTree)
if (showForeach) steps.push(executeForeach)

async.series(
  steps
, function(err, res){
    if (err) throw err

    if (tree.storage instanceof TrivialStore) {
      var cruft = Object.keys(tree.storage.things)
      console.log("cruft = %j", cruft)
      console.log("cruft.length = %d", cruft.length)
      console.log("tree.storage.nextId = %d", tree.storage.nextId)
    }
    if (tree.storage instanceof MemStore) {
      var cruft = Object.keys(tree.storage.buffers)
      console.log("cruft = %j", cruft)
      console.log("cruft.length = %d", cruft.length)
      console.log("tree.storage.nextId = %d", tree.storage.nextId)
    }

    if (showStats) {
      console.log("")
      console.log("STATS")
      console.log("=====")
//      console.log(Stats().get('bptree').toString())
      console.log(Stats().toString({values:"both"}))
//      console.log(BlockFile.STATS.toString())
    }

    console.log("THE END ")
  }
)

//THE END