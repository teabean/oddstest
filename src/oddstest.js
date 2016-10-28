const request = require('request')
const dotenv = require('dotenv')
const neo4j = require('neo4j-driver').v1

dotenv.load()

// Note my local neo4j has an uncrackable password :|
// const db = new neo4j.GraphDatabase('http://neo4j:password@localhost:7474')
const driver = neo4j.driver('bolt://localhost', neo4j.auth.basic('neo4j', 'password'))
const session = driver.session()

const pollInterval = 1000
const ident = process.env.ident
const password = process.env.password
let lastTimeStamp = -1

// Teardown
console.log('Removing existing graph database')
session.run('MATCH (n) DETACH DELETE n')
.then((result) => {
  console.log('Tear down complete')
  console.log('Creating Matches root node')
  session.run('CREATE (matches:Matches {description: {descriptionParam}}) RETURN matches', {descriptionParam: 'Matches'})
  .then((result) => {
    console.log('Matches node created')
    poll()
    session.close()
  })
  .catch((error) => {
    console.log(error)
    session.close()
  })
})
.catch((error) => {
  console.log(error)
  session.close()
})

const poll = () => {
  setTimeout(() => {
    // let url = `http://xml2.txodds.com/feed/odds/xml.php?ident=${ident}&passwd=${password}&mgid=1017&bid=17&ot=0&json=1`
    let url = `http://xml2.txodds.com/feed/odds/xml.php?ident=${ident}&passwd=${password}&mgstr=FBENG&bid=17&ot=0&all_odds=1&json=1`

    if (lastTimeStamp > 0) {
      url += `&last=${lastTimeStamp}`
    }

    console.log(url)

    request({
      uri: url,
      method: 'GET'
    }, (error, response, body) => {
      if (error) {
        console.log(error)
      } else if (response.statusCode === 200) {
        const json = JSON.parse(body)
        lastTimeStamp = json['@attributes'].timestamp
        let matches = json.match || []

        if (matches.length === 0) {
          console.log('No matches today')
        } else if (typeof (matches) === 'object') {
          // If we only have 1 match it returns the object and not an array with one object in it
          matches = [matches]
        }

        matches.map((match) => {
          const descriptionParam = `${match.hteam} vs ${match.ateam} - ${match.group}`
          const {bookmaker} = match
          const {offer} = bookmaker
          const attributes = match['@attributes']

          session.run(
            ' MATCH (matches:Matches)\n' +
            ' MERGE (match:Match {id: {idParam}, description: {descriptionParam}})\n' +
            ' MERGE (bookmaker:Bookmaker {id: {fakeBookmakerId}, bookmakerId: {bookmakerId}, description: {bookmakerDescription}})\n' +
            ' MERGE (offer:Offer {id: {offerId}, description: {offerDescription}})\n' +
            ' MERGE (match)-[:BELONGS_TO]->(matches)\n' +
            ' MERGE (bookmaker)-[:BELONGS_TO]->(match)\n' +
            ' MERGE (offer)-[:BELONGS_TO]->(bookmaker)\n' +
            ' RETURN match\n', {
              idParam: attributes.id,
              fakeBookmakerId: bookmaker['@attributes'].bid + '.' + attributes.id,
              descriptionParam,
              bookmakerId: bookmaker['@attributes'].bid,
              bookmakerDescription: bookmaker['@attributes'].name,
              offerId: offer['@attributes'].id,
              offerDescription: offer['@attributes'].otname
            })
          .then((result) => {
            console.log(`Match created ${descriptionParam}`)
            // Now add odds, they are in order newest -> oldest
            const {odds} = offer
            const offerId = offer['@attributes'].id
            let queryString = 'MATCH (offer:Offer {id: {offerId}})\n'

            if (odds.length > 0) {
              console.log('Creating odds...')
            }

            odds.map((o) => {
              const attributes = o['@attributes']
              const {
                time,
                i
              } = attributes
              const timestamp = new Date(time).getTime()

              // Need a fake id for this this as tx odds doesn't provide one but neo4j needs to distinguish between nodes
              queryString += ` MERGE (${'n' + i}:Odds {id: ${offerId}.${timestamp}, time: \'${time}\', timestamp: ${timestamp}})\n`

              // An odds object always has o1, o2 and o3 regardless of the odds type
              // Sometimes it will have an o4 if the odds type is AH
              queryString += ` MERGE (${'o1' + i}:Price {id: \'o1.${offerId}.${timestamp}\', value: ${o.o1}})\n`
              queryString += ` MERGE (${'o1' + i})-[:O1]->(${'n' + i})\n`
              queryString += ` MERGE (${'o2' + i}:Price {id: \'o2.${offerId}.${timestamp}\', value: ${o.o2}})\n`
              queryString += ` MERGE (${'o2' + i})-[:O2]->(${'n' + i})\n`
              queryString += ` MERGE (${'o3' + i}:Price {id: \'03.${offerId}.${timestamp}\', value: ${o.o3}})\n`
              queryString += ` MERGE (${'o3' + i})-[:O3]->(${'n' + i})\n`

              if (o.o4) {
                queryString += ` MERGE (${'o4' + i}:Price {id: \'o4.${offerId}.${timestamp}\', value: ${o.o4}})\n`
                queryString += ` MERGE (${'o4' + i})-[:O4]->(${'n' + i})\n`
              }

              // Now do Odds relationships to older odds
              if (i === '0') {
                // Create current price relationship
                queryString += ` MERGE (${'n' + i})-[:CURRENT_ODDS {timestamp: ${timestamp}}]->(offer)\n`
              } else {
                // Create Precedes relationship
                const prev = i - 1
                queryString += ` MERGE (${'n' + i})-[:PRECEDES]->(${'n' + prev})\n`
              }

              // Now need to see if we have 2 CURRENT_ODDS relationships and point the older one to the last node
              // we just made
              // queryString += ` WITH MATCH (current:Odds)-[r:CURRENT_ODDS]->(offer) WHERE r.timestamp != ${timestamp}`
              // queryString += ` DELETE r`
              // queryString += ` MERGE (current)-[:PRECEDEDS]->(${'n' + i})`
            })

            session.run(queryString, {
              offerId: offerId
            })
            .then((result) => {
              console.log('Odds created')
            })
            .catch((error) => {
              console.log(error)
            })
          })
          .catch((error) => {
            console.log(error)
          })
        })
      } else if (response.statusCode === 503) {
        // We have done too much
        console.log('Maximum request limit exceeded. Please try later.')
      } else {
        console.log('error: ' + response.statusCode)
        console.log(body)
      }
    })

    // poll()
  }, pollInterval)
}
