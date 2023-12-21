const express = require('express')
const app = express()
const processor = require('./processor.js')
const helpers = require('./helpers.js')

app.use(express.json())

const db = require('./db.js')

app.get('/', (req, res) => {
    res.end('false')
})

app.post('/addRequest', async (req, res) => {
    const {chat_id, url, method, host, ...params} = req.body

    res.end(
        JSON.stringify(
            await db.addRequest(db.requestsTableName, {
                chat_id: chat_id,
                url: url,
                method: method,
                host: host,
                params: JSON.stringify({...params, chat_id: chat_id}),
                status: 0,
                time: helpers.toSqlDateString(new Date()),
                token: url.split('/').find(chunk => chunk.includes('bot')).split('bot')[1]
            })
        )
    )
})

app.post('/bulkStatusChange', async (req, res) => {
    const {host, data, newStatus} = req.body

    res.end(
        newStatus ?
            JSON.stringify(
                await db.addRequest(db.bulkStatusChangeTableName, {
                    host: host,
                    data: JSON.stringify(data),
                    time: helpers.toSqlDateString(new Date()),
                    status: 0,
                    transition_status: newStatus
                })
            ) : {ok: true}
    )
})

app.listen(3000, async () => {
    console.log ('Server is listening on port 3000')
    processor.runRequestHandler()
    processor.runChangedOrderStatusesRequestHandler()
    processor.runClearDBSchedule()
})

// report error to group
process.on('uncaughtException', async (err, origin) => {
    await helpers.sendErrorToGroup(err, origin)
    processor.runRequestHandler()
    processor.runChangedOrderStatusesRequestHandler()
    processor.runClearDBSchedule()
})