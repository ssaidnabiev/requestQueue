const pool = require('./pool');
const helpers = require('./helpers.js')

const requestsTableName = 'requests'
const bulkStatusChangeTableName = 'changed_order_statuses'

// async function newPgClient() {
//     const pgClient = new pg.Client(pgDbDetails)
//     pgClient.on('error', async error => {
//         await helpers.sendErrorToGroup(error)
//         await pgClient.end()
//     })
//     await pgClient.connect()
//     return pgClient
// }

async function addRequest(tableName, newRow) {

    // const pgClient = await newPgClient()

    try {
        const values = []
        const text = `insert into ${tableName}(${
            Object.keys(newRow).map(key => {
                values.push(newRow[key])
                return `${key}`
            }).join(',')
        }) values (${
            values.map ((val, index) => {
                return `$${index + 1}`
            }).join(',')
        })`

        await pool.query({text: text, values: values})
        
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> addRequest()')
        // await pgClient.end()
        return {ok: false, error_code: 500, description: error.message}
    }

    // await pgClient.end()
    return {ok: true}
}

async function updateRequest(tableName, id, newRow) {
    // const pgClient = await newPgClient()

    let colCount = 1
    const values = [id]
    const text = `update ${tableName} set ${
        Object.keys(newRow).map(key => {
            colCount++
            values.push(newRow[key])
            return `${key}=$${colCount}`
        }).join(',')
    } where id=$1`

    try {
        await pool.query({text: text, values: values})
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> updateRequest()')
    }
    
    // await pgClient.end()
}

async function getRequests(tableName) {
    const sql = `
        select r.*
        from ${tableName} r
        where r.id in (
            select min(r2.id) 
            from ${tableName} r2
            where r2.status = 0
            group by r2.chat_id, r2.token
        )
        and (r.update is null or r.update < (now() - interval '1 second'))
        order by r.id asc
        limit 50`;
    
    try {
        const result = await pool.query(sql);
        return result;
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> getRequests()');
        return false;
    }
}

async function getDisabledChatRequests(tableName) {
    const sql = `
        select r.*
        from ${tableName} r
        where status = -2
        and (r.update is null or r.update < (now() - interval '10 seconds'))
        limit 50`;
    
    try {
        const result = await pool.query(sql);
        return result;
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> getDisabledChatRequests()');
        return false;
    }
}

async function getBulkStatusChanges(tableName) {
    const sql = `
        select 
            b.*
        from ${tableName} b
        where b.status = 0
        and (b.update is null or b.update < (now() - interval '2 seconds'))
        order by b.id asc
        limit 50`;
    
    try {
        const result = await pool.query(sql);
        return result;
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> getBulkStatusChanges()');
        return false;
    }
}

async function clearDatabase(tableName) {
    // const pgClient = await newPgClient()
    try {
        const sql = `delete from ${tableName} where time < (now()-interval '1 day')::timestamp`
        await pool.query(sql)
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'db.js -> clearDatabase')
    }
    
    // await pgClient.end()
}


module.exports = {addRequest, updateRequest, requestsTableName, bulkStatusChangeTableName, getRequests, getBulkStatusChanges, clearDatabase, getDisabledChatRequests}