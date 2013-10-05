/* global describe it */

"use strict";

var assert = require('assert')
  , BpTree = require('..')
  , TrivialStore = require('../lib/trivial_store')
  , MemStore = require('../lib/mem_store')
  , strOps = require('../lib/str_ops')
  , async = require('async')
  , format = require('util').format


var val
  , key = ""
  , kvs = []

for (val=0; val<100; val++) {
  key = strOps.inc(key)
  kvs.push([ key, val ])
}


describe("BpTree-TrivialStore order=3; Insert & Delete 100 k/v pairs", function(){
  var bpt = new BpTree(strOps.cmp, new TrivialStore(), {order: 3})

  it("should insert an array k/v pairs", function(done){
    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.put(k, kvs[k], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.del(k, mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(bpt.root == null) //bpt is empty
                       done()
                     })
  })

  it("the internal stoage should be empty", function(){
    assert.ok( Object.keys(bpt.storage.things).length == 0
             , format("Object.keys(bpt.storage.things)= %j"
                     , Object.keys(bpt.storage.things)) )
  })
})


describe("BpTree-MemStore order=3; Insert & Delete 100 k/v pairs", function(){
  var bpt = new BpTree(strOps.cmp, new MemStore(), {order: 3})

  it("should insert an array k/v pairs", function(done){
    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.put(k , kvs[k], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       bpt.del(k, mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       assert.ok(bpt.root == null) //bpt is empty
                       done()
                     })
  })

  it("the internal stoage should be empty", function(){
    assert.ok( Object.keys(bpt.storage.buffers).length == 0
             , format("Object.keys(bpt.storage.buffers)= %j"
                     , Object.keys(bpt.storage.buffers)) )
  })

})

//describe("", function(){
//  it("", function(done){
//  })
//})
