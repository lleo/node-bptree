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
  , strCmp = require('../lib/str_cmp')
  , TrivialStore = require('../lib/trivial_store')

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

function rand(n) { return Math.random() * n } // [0,n)
function randInt(n) { return Math.floor( rand(n) ) } //[0,n)
function randomize(arr) {
  var i, ri, idx = []
  for (i=0; i<arr.length; i++) idx.push(i)

  function swap(a, b){
    var t = arr[a]
    arr[a] = arr[b]
    arr[b] = t
  }

  for(i=0; i<arr.length; i++) {
    ri = idx[randInt(idx.length)]
    idx.splice(ri,1)
    swap(i, ri)
  }
}

function incStr(str){
  if (str.length == 0) return "a"

  var last = str[str.length-1]
    , rest = str.substr(0, str.length-1)

  if (last == "z") {
    if (rest.length == 0) return "aa"
    else return incStr(rest) + "a"
  }
  if (last == "Z") {
    if (rest.length == 0) return "AA"
    else return incStr(rest) + "A"
  }
  if (last == "9") {
    if (rest.length == 0) return "10"
    else return incStr(rest) + "0"
  }

  return rest + String.fromCharCode( last.charCodeAt(0)+1 )
}

function multStr(str, times){
  return Array((times?times:0)+1).join(str)
}

//DEL:function throwErr(err) { if (err) throw err }
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


var trivialStore = new TrivialStore()
  , order = 3
  , tree = new BpTree(order, trivialStore, strCmp)
  , strSeed = ""
  , keys = []
  , dKeys

for (var ki = 0; ki<numEnts; ki++)
  keys.push( strSeed = incStr(strSeed) )
//for (var ki = 0; ki<numEnts; ki++)
//  console.log("key[%d] = %s", ki, keys[ki])

dKeys = u.clone(keys)
randomize(dKeys)

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
                   tree.del(key, function(err){
                     if (err) { scb(err); return }
                     tree.traverseInOrder(displayNode, scb)
                   })
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