/* global describe it */

"use strict";

var assert = require('assert')
  , BpTree = require('..')
  , MemStore = require('../lib/mem_store')
  , Leaf = BpTree.Leaf
  , u = require('lodash')
  , async = require('async')
  , format = require('util').format

describe("BpTree", function(){
  var tree, order=3
    , keys = []
    , data = []
    , cur = 0
    , key = cur.toString()
    , val = cur

  describe("Constructor w/MemStore", function(){
    tree = new BpTree(order, null, new MemStore(1))

    it("should construct an object", function(){
      assert.ok(tree instanceof BpTree)
    })

  })

  describe("put/get first data", function(){

    it("should put first key/data", function(next){
      keys.push(key)
      data.push(val)
      tree.put(key, val, next)
    })

    it("should get first key/data", function(next){
      tree.get(keys[0], function(err, d){
        if (err) throw err
        assert.equal(d, data[0])
        next()
      })
    })

    it("use .findLeaf to find the leaf and path", function(next){
      tree.findLeaf('0', function(err, leaf, path){
        if (err) { next(err); return }
        if ( path.length != 0 )
          next(new Error("path not zero"))
        else if ( !(leaf instanceof Leaf) )
          next(new Error("leaf not a Leaf"))
        else
          next()
      })
    })

    it("put the next two k/v pairs; splitting the root leaf", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 1
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 2
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) { next(err); return }

          //use the last key/val pair
          tree.findLeaf(key, function(err, leaf, path){
            if (path.length != 1)
              next(new Error("path is not 1"))
            else
              next()
          })
        })
    })

    it("put a four more k/v pairs, resulting in another b-tree level", function(next){
      async.series(
        [ function(scb){
            cur += 1 //cur is now = 3
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 4
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 5
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        , function(scb){
            cur += 1 //cur is now = 6
            key = cur.toString()
            val = cur

            keys.push(key)
            data.push(val)
            tree.put(key, val, scb)
          }
        ]
      , function(err, res) {
          if (err) next(err)

          //use the last key/val pair
          tree.findLeaf(key, function(err, leaf, path){
            if (err) { next(err); return }

            if (path.length != 2)
              next(new Error("path is not 2"))
            else
              next()
          })
        })//async.series
    })//it
  })//describe put/get first data

  describe("delete all nodes", function(){
    it("should delete all the keys", function(next){
      async.mapSeries(
        keys
        , function(key, mcb){
           tree.del(key, mcb)
         }
      , function(err, res){
          if (err) { next(err); return }
          assert.ok( u.isEqual(data, res) )
          next()
        })
    })
  })
})//BpTree

//the end