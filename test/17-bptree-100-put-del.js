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


describe("BpTree-TrivialStore order=3; Insert & Delete 100 k/v pairs", function(){
  var t = new BpTree(3, strOps.cmp, new TrivialStore())

  it("should insert an array k/v pairs", function(done){
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
})


describe("BpTree-MemStore order=3; Insert & Delete 100 k/v pairs", function(){
  var t = new BpTree(3, strOps.cmp, new MemStore())

  it("should insert an array k/v pairs", function(done){
    async.mapSeries( Object.keys(kvs)
                   , function(k, mcb){
                       t.put(k , kvs[k], mcb)
                     }
                   , function(err, res){
                       if (err) { done(err); return }
                       done()
                     })
  })

  it("should delete an array k/v pairs", function(done){
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
})

//describe("", function(){
//  it("", function(done){
//  })
//})
