
var assert = require('assert')
  , u = require('lodash')
  , Branch = require('./branch')
//  , Leaf = require('./leaf')

exports = module.exports = OpData
function OpData(type){
  this.type = type
//  this._path   = []
  this._stored = []
  this._loaded = []
  this._deadHdls = []
}


//OpData.prototype.path = function(){
//  return u.clone(this._path)
//}
//
//
//OpData.prototype.atRoot = function(){
//  return this._path.length == 0
//}
//
//
//OpData.prototype.addParent = function(branch){
//  assert.ok(branch instanceof Branch)
//  this._path.push(branch)
//}
//
//
//OpData.prototype.peekParent = function(){
//  return this._path[this._path.length-1]
//}
//
//
//OpData.prototype.getParent = function(){
//  return this._path.pop()
//}


OpData.prototype.stored = function(oldHdl, newHdl){
  this._stored.push([oldHdl, newHdl])
}


OpData.prototype.loaded = function(hdl, n){
  this._loaded.push([hdl,n])
}


OpData.prototype.deadHdl = function(hdl){
  this._deadHdls.push(hdl)
}


OpData.prototype.deadHdls = function(){
  var deadHdls = u.clone(this._deadHdls)

  for (var i=0; i<this._stored.length; i+=1) {
    var pair = this._stored[i]
      , oldHdl = pair[0]
      , newHdl = pair[1]

    if ( oldHdl != null && !oldHdl.equals(newHdl) )
      deadHdls.push(oldHdl)
  }

  return deadHdls
}
//