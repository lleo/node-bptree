/* global module exports require setTimeout */

var assert = require('assert')

exports = module.exports = BlockStore
function BlockStore(file) {
  this.file = file
  this.freelist = []   //FIXME: calculate frome file
  this.cur_segment = 0 //FIXME: fill this in
}

BlockStore.open = function(filename, cb){

}

BlockStore.prototype.nextHandle = function(){
  if (this.freelist.length > 0) {
    return this.freelist.pop()
  }


}

BlockStore.prototype.store = function(handle, buf, cb){

}

BlockStore.prototype.load = function(handle, cb){

}

BlockStore.prototype.release = function(handle, cb){

}