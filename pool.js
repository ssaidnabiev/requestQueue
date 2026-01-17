const { Pool }  = require('pg')
const helpers = require('./helpers.js')
const info = require('./info.js')

const pgDbDetails = {
    user: info.dbUser,
    host: info.dbHost,
    database: info.dbName,
    password: info.dbPassword,
    port: 5432,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000
}

const pool = new Pool(pgDbDetails);

pool.on('error', async (err) => {
  await helpers.sendErrorToGroup(err, 'pool.js -> pool.on("error", async() => {...})');
  console.error('Unexpected PG pool error:', err);
});

process.on('SIGINT', async () => {
  console.log('Closing DB pool...');
  await pool.end();
  process.exit(0);
});

module.exports = pool;
