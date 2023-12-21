const pg = require('pg')
const helpers = require('./helpers.js')

const pgDbDetails = {
    user: 'postgres',
    host: 'localhost',
    database: 'postgres',
    password: 'M9[eL4*Bd%G`Q~~q',
    port: 5432
}

const requestsTableName = 'requests'
const bulkStatusChangeTableName = 'changed_order_statuses'


async function newPgClient() {
    const pgClient = new pg.Client(pgDbDetails)
    await pgClient.connect()
    return pgClient
}

async function addRequest(tableName, newRow) {

    const pgClient = await newPgClient()

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

        await pgClient.query({text: text, values: values})
        
    } catch (error) {
        await helpers.sendErrorToGroup(error)
        await pgClient.end()
        return {ok: false, error_code: 500, description: error.message}
    }

    await pgClient.end()
    return {ok: true}
}

async function updateRequest(tableName, id, newRow) {
    const pgClient = await newPgClient()

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
        await pgClient.query({text: text, values: values})
    } catch (error) {
        await helpers.sendErrorToGroup(error)
    }
    
    await pgClient.end()
}

async function getRequests(tableName) {
    const pgClient = await newPgClient()
    const sql =
        `select 
            r.*
        from ${tableName} r
        where r.id in (
            select min(r2.id) 
            from ${tableName} r2
            where r2.status = 0
            group by r2.chat_id
        )`
    
    try {
        const result = await pgClient.query(sql)
        await pgClient.end()
        return result
    } catch (error) {
        await helpers.sendErrorToGroup(error)
        await pgClient.end()
        return false
    }
}

async function getBulkStatusChanges(tableName) {
    const pgClient = await newPgClient()
    const sql =
        `select 
            b.*
        from ${tableName} b
        where b.id in (
            select min(b2.id) 
            from ${tableName} b2
            where b2.status = 0
        )`
    
    try {
        const result = await pgClient.query(sql)
        await pgClient.end()
        return result
    } catch (error) {
        await helpers.sendErrorToGroup(error)
        await pgClient.end()
        return false
    }
}

async function clearDatabase(tableName) {
    const pgClient = await newPgClient()
    try {
        const sql = `delete from ${tableName} where time < (now()-interval '1 day')::timestamp`
        await pgClient.query(sql)
    } catch (error) {
        await helpers.sendErrorToGroup(error)
    }
    
    await pgClient.end()
}


module.exports = {newPgClient, addRequest, updateRequest, requestsTableName, bulkStatusChangeTableName, getRequests, getBulkStatusChanges, clearDatabase}