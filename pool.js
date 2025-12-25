const { Pool }  = require('pg')
const helpers = require('./helpers.js')

const pgDbDetails = {
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'M9[eL4*Bd%G`Q~~q',
    port: 5432,
    max: 20,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000
}

const pool = new Pool(pgDbDetails);

pool.on('error', async (err) => {
  await helpers.sendErrorToGroup(err);
  console.error('Unexpected PG pool error:', err);
});

process.on('SIGINT', async () => {
  console.log('Closing DB pool...');
  await pool.end();
  process.exit(0);
});

module.exports = pool;
