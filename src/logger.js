'use strict'

module.exports = function log () {
  const ts = new Date().toISOString().replace('T', ' ').replace('Z', '')

  console.log(ts, ...arguments)
}
