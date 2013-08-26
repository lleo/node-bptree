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
  , MemStore = require('../lib/mem_store')

function usage(msgs, xit) {
  if (typeof msgs === 'string') msgs = [msgs]
  var out = xit !== 0 ? process.stderr : process.stdout
  msgs.unshift("Usage: test.js <numEnts>")
  msgs.forEach(function(m){ out.write(m+'\n') })
  process.exit(xit)
}
if (process.argv.length !== 3) usage("wrong number of arguments", 1)

var numEnts = parseInt( process.argv[2], 10 )
if (numEnts % 1 != 0) usage(format("numEnts not an Integer %d", numEnts), 1)

console.log("numEnts = %d", numEnts)
//process.exit(0)

function logStr(err, str) {
  if (err) throw err
  console.log(str)
}

var displayNode = (
  function(){
    var cnt=0
    return function(node, isLeaf, depth){
      var indent = depth>0 ? multStr("  ", depth) : ""
      console.log("%s%s", indent, node)
      return cnt++
    }
  }
)()

var order = 3
  , ms = new MemStore(1)
  , tree = new BpTree(order, strCmp, ms)
  , strSeed = ""
  , keys = []
  , dKeys

for (var ki = 0; ki<numEnts; ki++)
  keys.push( strSeed = incStr(strSeed) )
//for (var ki = 0; ki<numEnts; ki++)
//  console.log("key[%d] = %s", ki, keys[ki])

dKeys = u.clone(keys)
//randomize(dKeys)

//console.log( "[ %s ].length = %d"
//           , keys.map(function(k){return k.toString()}).join("\n, ")
//           , keys.length)
//console.log( "[ %s ].length = %d"
//           , dKeys.map(function(k){return k.toString()}).join("\n, ")
//           , dKeys.length)
//process.exit(0)

var delSteps = dKeys
               .map(function(key){
                 return function(scb){
                   console.log("")
                   console.log("Modify Tree")
                   console.log("-----------")
                   console.log("tree.del(%s)", key)

                   //delay this del & display to allow stdout to flush
                   setTimeout(function(){
                     tree.del(key, function(err){
                       if (err) { scb(err); return }
                       tree.traverseInOrder(displayNode, scb)
                     })
                   }, 100)
                 }
               })
  , buildTree =  function(scb){
      console.log("Build Tree: %d tree.put() calls", keys.length)
      console.log("----------")
      var i=0
      async.whilst(
        /*test*/
        function(){ return i < keys.length }
        /*body*/
      , function(next) {
          var iKey = keys[i]
          console.log("tree.put( %s, %d )", iKey, i)
          tree.put(iKey, i, next)
          //tree.put(iKey, i, function(err, ids){
          //  if (err) { next(err); return }
          //  tree.traverseInOrder(displayNode, next)
          //  //next(null, ids)
          //})
          i+=1;
        }
        /*finnaly*/
        //, scb
      , function(err){
          if (err) throw err
          //display in-order
          console.log("")
          console.log("Initial Tree")
          console.log("------------")
          tree.traverseInOrder(displayNode, scb)
        }
      )
    }
  , lastSteps = [ function(scb){
                    console.log("FINAL TREE")
                    console.log("----------")
                    tree.traverseInOrder(displayNode, function(err, res){
                      if (err) console.log("err = %j", err)
                      else     console.log("res = %j", u.flatten(res))
                      scb(null)
                    })
                  }
                , function(scb){
                    console.log("")
                    console.log("FOREACH(key, val)")
                    console.log("-----------------")
                    tree.forEach(function(k,v){
                      console.log("k = %s; v = %s;", k, v)
                    }, scb)
                  }
              ]
  , steps = u.flatten([ buildTree
                      , delSteps
                      , lastSteps
                      ])

async.series(
  steps
, function(err, res){
    if (err) throw err

    //console.log("res = %j", u.flatten(res)) //results of
    //console.log("")

    console.log("Node.CNT = %d", Node.CNT)
    console.log("THE END ")
  }
)

//THE END