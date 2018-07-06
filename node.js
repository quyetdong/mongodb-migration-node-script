import parallel from 'async/parallel';
import mongodb from 'mongodb';

import customers from './m3-customer-data.json';
import customerAddresses from './m3-customer-address-data.json';

const limit = parseInt(process.argv[2], 10) || 1000;
const { MongoClient, Server } = mongodb;

// Connect to mongodb
const mongoClient = new MongoClient(new Server('localhost', 27017))

let tasks = [];
// Connect to edx-course-db database and work with it
mongoClient.connect((error, client) => {
  if (error) return process.exit(1)
  // get database name and connect to the database
  const dbName = 'edx-course-db';
  const db = client.db(dbName);

  customers.forEach((customer, index, list) => {
    customers[index] = Object.assign(customer, customerAddresses[index])

    if (index % limit === 0) {
      const start = index;
      const end = (start + limit > customers.length) ? customers.length-1 : start+limit-1;

      tasks.push((done) => {
        console.log(`Processing ${start}-${end} out of ${customers.length}`)
        db.collection('customers').insert(customers.slice(start, end+1), (error, results) => {
          done(error, results)
        })
      })
    }
  })

  console.log(`Launching ${tasks.length} parallel task(s)`);
  const startTime = Date.now();

  // clear the database before insert new data
  db.collection('customers').deleteMany({}, (error, result) => {
    if (error) return process.exit(1)
    console.log(result.result.n, 'deleted') 
    parallel(tasks, (error, results) => {
      if (error) console.error(error)
      const endTime = Date.now()
      console.log(`Execution time: ${endTime-startTime}`)
      // console.log(results)
      db.collection('customers').find({}).toArray((error, results) => {
        if (error) return process.exit(1)
        console.log('inserted', results.length);
        client.close()
      });
    })
  })
})
