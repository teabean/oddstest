'use strict'

const express = require('express')
const dotenv = require('dotenv')
const app = express()
const bodyParser = require('body-parser')
const config = require('../config')

dotenv.load()

app.set('json spaces', 2)

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: true }))

const url = 'http://xml2.txodds.com/feed/odds/xml.php?'

// Get last timestamp from db

// If greater than 1 hour get everything

// Else use the timestamp to lookup data

// Slee 30 seconds

// http://xml2.txodds.com/feed/odds/xml.php?ident=betica&passwd=567678789&mgid=1018&bid=17&ot=0&json=1
console.log(process.env.password)

app.use((req, res, next) => {
  res.status(404)
  console.log('BAD REQUEST', req.method, req.url)
  res.type('txt').send('Not found')
})

app.listen(config.port, '0.0.0.0', () => {
  console.log(`Listening on port ${config.port}`)
})
