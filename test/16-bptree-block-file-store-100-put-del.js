/* global describe it */

"use strict";

var assert = require('assert')
  , BpTree = require('..')
  , TrivialStore = require('../lib/trivial_store')
  , BlockFileStore = require('../lib/block_file_store')
  , strOps = require('../lib/str_ops')
  , async = require('async')
  , block_file_fn = './test-bptree-100.bf'
  , fs = require('fs')

var val
  , key = ""
  , kvs = []

for (val=0; val<100; val++) {
  key = strOps.inc(key)
  kvs.push([ key, val ])
}

try { fs.unlinkSync(block_file_fn) }
catch (x) {  }


describe("BpTree-BlockFileStore order=3; Insert & Delete 100 k/v pairs", function(){
  var bpt

  it("should open and create a BpTree w/BlockFileStore", function(done){
    BlockFileStore.create(block_file_fn, function(err, bfs){
      if (err) { done(err); return }
      bpt = new BpTree(strOps.cmp, bfs, {order: 3})
      bpt.loadRoot(done)
    })
  })

  it("should insert an array k/v pairs", function(done){
//    this.timeout(10000)

    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.put(k, kvs[k], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should .close the BlockFile", function(done){
    bpt.storage.close(function(err){
      if (err) { done(err); return }
      bpt = undefined
      done()
    })
  })

  it("should re-open the BpTree w/BlockFileStore", function(done){
    BlockFileStore.open(block_file_fn, function(err, bfs){
      if (err) { done(err); return }
      bpt = new BpTree(strOps.cmp, bfs, {order: 3})
      bpt.loadRoot(done)
    })
  })

  it("should delete an array k/v pairs", function(done){
//    this.timeout(10000)

    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.del(k, mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(bpt.root == null) //tree is empty
                       done()
                     })
  })

  it("should .close the BlockFile", function(done){
    bpt.storage.close(function(err){
      if (err) { done(err); return }
      bpt = undefined
      done()
    })
  })

  it("should re-open the BpTree w/BlockFileStore", function(done){
    BlockFileStore.open(block_file_fn, function(err, bfs){
      if (err) { done(err); return }
      bpt = new BpTree(strOps.cmp, bfs, {order: 3})
      bpt.loadRoot(done)
    })
  })

  it("should be empty; bpt.root === null", function(){
    assert.ok( bpt.root === null )
  })

  it("should close and delete the block_file_fn="+block_file_fn, function(done){
    bpt.storage.close(function(err){
      if (err) { done(err); return }
      fs.unlinkSync(block_file_fn)
      bpt = undefined
      done()
    })
  })
})

//describe("", function(){
//  it("", function(done){
//  })
//})
