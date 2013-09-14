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


describe("BpTree-BlockFileStore order=3; Insert & Delete 100 k/v pairs", function(){
  var t, bfs

  it("should open and create a BpTree w/BlockFileStore", function(done){
    BlockFileStore.open(block_file_fn, function(err, bfs_){
      if (err) { done(err); return }
      bfs = bfs_
      t = new BpTree(3, strOps.cmp, bfs)
      done()
    })

  })

  it("should insert an array k/v pairs", function(done){
//    this.timeout(10000)

    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       t.put(k, kvs[k], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
//    this.timeout(10000)

    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       t.del(k, mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(t.root == null) //tree is empty
                       done()
                     })
  })

  it("should close and delete the block_file_fn="+block_file_fn, function(done){
    fs.unlinkSync(block_file_fn)
    bfs.close(done)
  })
})

//describe("", function(){
//  it("", function(done){
//  })
//})
