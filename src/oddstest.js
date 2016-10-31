const log = require('./logger')
const request = require('request')
const dotenv = require('dotenv')
const neo4j = require('neo4j-driver').v1

dotenv.load()

const neo4juser = process.env.neo4juser
const neo4jpassword = process.env.neo4jpassword
const driver = neo4j.driver('bolt://localhost', neo4j.auth.basic(neo4juser, neo4jpassword))
const session = driver.session()

const pollInterval = 10000
const ident = process.env.ident
const password = process.env.password
let lastTimeStamp = -1

// Teardown
log('Tear down of existing graph database...')
session.run('MATCH (n) DETACH DELETE n')
.then((result) => {
  log('Tear down complete')
  log('Creating Matches root node...')
  session.run('CREATE (matches:Matches {description: {descriptionParam}}) RETURN matches', {descriptionParam: 'Matches'})
  .then((result) => {
    log('Matches node created')
    poll()
    session.close()
  })
  .catch((error) => {
    log(error)
    session.close()
  })
})
.catch((error) => {
  log(error)
  session.close()
})

const poll = () => {
  setTimeout(() => {
    // let url = `http://xml2.txodds.com/feed/odds/xml.php?ident=${ident}&passwd=${password}&mgid=1017&bid=17&ot=0&json=1`
    let url = `http://xml2.txodds.com/feed/odds/xml.php?ident=${ident}&passwd=${password}&mgstr=FBENG&bid=17,42,126&days=1&ot=0&all_odds=1&json=1`

    if (lastTimeStamp > 0) {
      url += `&last=${lastTimeStamp}`
    }

    log('Requesting data...')

    request({
      uri: url,
      method: 'GET'
    }, (error, response, body) => {
      if (error) {
        log(error)
      } else if (response.statusCode === 200) {
        log('Response received')
        const json = JSON.parse(body)
        lastTimeStamp = json['@attributes'].timestamp
        let matches = json.match || []

        if (matches.length === 0) {
          log('Nothing new')
        } else if (!Array.isArray(matches)) {
          // If we only have 1 match it returns the object and not an array with one object in it
          matches = [matches]
        }

        matches.map((match) => {
          log('Creating match node...')
          const descriptionParam = `${match.hteam} vs ${match.ateam} - ${match.group}`
          let bookmakers = match.bookmaker

          if (!Array.isArray(bookmakers)) {
            bookmakers = [bookmakers]
          }

          bookmakers.map((bookmaker) => {
            let offers = bookmaker.offer

            if (!Array.isArray(offers)) {
              offers = [offers]
            }

            offers.map((offer) => {
              const attributes = match['@attributes']

              session.run(
                ' MATCH (matches:Matches)' +
                ' MERGE (match:Match {id: {idParam}, description: {descriptionParam}})' +
                ' MERGE (bookmaker:Bookmaker {id: {fakeBookmakerId}, bookmakerId: {bookmakerId}, description: {bookmakerDescription}, startTime: {startTime}})' +
                ' MERGE (offer:Offer {id: {offerId}, description: {offerDescription}})' +
                ' MERGE (match)-[:BELONGS_TO]->(matches)' +
                ' MERGE (bookmaker)-[:BELONGS_TO]->(match)' +
                ' MERGE (offer)-[:BELONGS_TO]->(bookmaker)' +
                ' RETURN match', {
                  idParam: attributes.id,
                  fakeBookmakerId: bookmaker['@attributes'].bid + '.' + attributes.id,
                  descriptionParam,
                  startTime: match.time,
                  bookmakerId: bookmaker['@attributes'].bid,
                  bookmakerDescription: bookmaker['@attributes'].name,
                  offerId: offer['@attributes'].id,
                  offerDescription: offer['@attributes'].otname
                })
              .then((result) => {
                log(`Match node created ${descriptionParam}`)
                // Now add odds, they are in order newest -> oldest
                let {odds} = offer
                const offerId = offer['@attributes'].id
                let queryString = `MATCH (offer:Offer {id: {offerId}})`

                let latestOddsTime

                if (!Array.isArray(odds)) {
                  // Same as with matches, if there is only one odds object they are not in an Array
                  odds = [odds]
                }

                if (odds.length === 0) {
                  log('No odds for offer')
                }

                odds.map((o) => {
                  const attributes = o['@attributes']
                  const {
                    time,
                    i
                  } = attributes

                  const timestamp = new Date(time).getTime()

                  // Need a fake id for this this as tx odds doesn't provide one but neo4j needs to distinguish between nodes
                  queryString += ` MERGE (${'n' + i}:Odds {id: \'${offerId}.${timestamp}\', time: \'${time}\', timestamp: ${timestamp}})`

                  // An odds object always has o1, o2 and o3 regardless of the odds type
                  // Sometimes it will have an o4 if the odds type is AH
                  queryString += ` MERGE (${'o1' + i}:Price {id: \'o1.${offerId}.${timestamp}\', value: ${o.o1}})`
                  queryString += ` MERGE (${'o1' + i})-[:O1]->(${'n' + i})`
                  queryString += ` MERGE (${'o2' + i}:Price {id: \'o2.${offerId}.${timestamp}\', value: ${o.o2}})`
                  queryString += ` MERGE (${'o2' + i})-[:O2]->(${'n' + i})`
                  queryString += ` MERGE (${'o3' + i}:Price {id: \'o3.${offerId}.${timestamp}\', value: ${o.o3}})`
                  queryString += ` MERGE (${'o3' + i})-[:O3]->(${'n' + i})`

                  if (o.o4) {
                    queryString += ` MERGE (${'o4' + i}:Price {id: \'o4.${offerId}.${timestamp}\', value: ${o.o4}})`
                    queryString += ` MERGE (${'o4' + i})-[:O4]->(${'n' + i})`
                  }

                  // Now do Odds relationships to older odds
                  if (i === '0') {
                    latestOddsTime = timestamp
                    // Create current price relationship
                    queryString += ` MERGE (${'n' + i})-[:CURRENT_ODDS {timestamp: ${timestamp}}]->(offer)`
                  } else {
                    // Create Precedes relationship
                    const prev = i - 1
                    queryString += ` MERGE (${'n' + i})-[:PRECEDES]->(${'n' + prev})`
                  }
                })

                if (odds.length > 0) {
                  // Now need to see if we have 2 CURRENT_ODDS relationships and point the older one to the last node
                  // we just made
                  queryString += ` WITH offer as offer `
                  queryString += ` MATCH (current:Odds)-[r:CURRENT_ODDS]->(offer) WHERE r.timestamp <> ${latestOddsTime}`
                  queryString += ` DELETE r`
                  queryString += ` MERGE (current)-[:PRECEDES]->(${'n' + odds.length})`
                }

                // log(queryString)

                session.run(queryString, {
                  offerId: offerId
                })
                .then((result) => {
                  log('Odds created')
                })
                .catch((error) => {
                  log(error)
                })
              })
              .catch((error) => {
                log(error)
              })
            })
          })
        })
      } else if (response.statusCode === 503) {
        // We have done too much
        log('Maximum request limit exceeded. Please try later.')
      } else {
        log('error: ' + response.statusCode)
        log(body)
      }
    })

    poll()
  }, pollInterval)
}
