
exports.log = log
function log(){
  console.log.apply(console, arguments)
}