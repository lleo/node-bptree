/* global describe it */

"use strict";

var assert = require('assert')
  , BpTree = require('..')
  , TrivialStore = require('../lib/trivial_store')
  , MemStore = require('../lib/mem_store')
  , strOps = require('../lib/str_ops')
  , async = require('async')


var val
  , key = ""
  , kvs = []

for (val=0; val<100; val++) {
  key = strOps.inc(key)
  kvs.push([ key, val ])
}


describe("BpTree-TrivialStore order=3", function(){
  var t = new BpTree(3, strOps.cmp, new TrivialStore())

  it("should insert an array k/v pairs", function(done){
    async.mapSeries( kvs
                   , function(kv, mcb){
                       t.put(kv[0], kv[1], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
    async.mapSeries( kvs
                   , function(kv, mcb){
                       t.del(kv[0], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(t.root == null) //tree is empty
                     })
  })
})


describe("BpTree-MemStore order=3", function(){
  var t = new BpTree(3, strOps.cmp, new MemStore())

  it("should insert an array k/v pairs", function(done){
    async.mapSeries( kvs
                   , function(kv, mcb){
                       t.put(kv[0], kv[1], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
    async.mapSeries( kvs
                   , function(kv, mcb){
                       t.del(kv[0], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(t.root == null) //tree is empty
                     })
  })
})

//describe("", function(){
//  it("", function(done){
//  })
//})
