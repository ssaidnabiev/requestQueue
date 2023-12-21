const db = require('./db.js')
const helpers = require('./helpers.js')
const schedule = require('node-schedule')

const processNextQueue = async () => {

    const result = await db.getRequests(db.requestsTableName)

    if (result === false) {return}

    const responsePromises = result.rows.map(async row => {
        try {
            const responsePromise = await fetch(`${row.url}${row.method}`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: row.params
            })
            return responsePromise
        } catch (error) {
            return Promise.resolve({fetchFailed: true, error: error})
        }
        
    })

    const responses = await Promise.all(responsePromises)

    const decodePromises = responses.map(async response => {
        if (!response.fetchFailed) {
            return await response.json()
        } else {
            return Promise.resolve({ok:false, error_code: 500, description: response.error.message})
        }
    })

    const decodedRows = await Promise.all(decodePromises)

    const dbWritePromises = decodedRows.map(async (decoded, index) => {
        const {id, ...row} = result.rows[index]
        if (decoded.ok) {
            row.status = 1
        } else {
            if (decoded.error_code == 400) {
                const params = JSON.parse(row.params)
                if (params.media) {

                    let skipMedia = false
                    const mediaParams = params.media

                    params.media.forEach((media, index) => {

                        if (skipMedia) {return}

                        const urlSplitted = media.media.split('?')
                        const queryString = urlSplitted.length > 1 ? urlSplitted[1] : '?attempt=0'
                        const urlParams = new URLSearchParams(queryString)
                        const currentAttempt = parseInt(urlParams.get('attempt')) + 1

                        if (currentAttempt >= 3) {
                            row.status = -1
                            row.error = `${decoded.error_code}: ${decoded.description}`
                            skipMedia = true
                        } else {
                            mediaParams[index].media = `${urlSplitted[0]}?attempt=${currentAttempt}`
                        }

                    })

                    params.media = mediaParams
                    row.params = JSON.stringify(params)
                } else if (params.photo) {

                    const urlSplitted = params.photo.split('?')
                    const queryString = urlSplitted.length > 1 ? urlSplitted[1] : '?attempt=0'
                    const urlParams = new URLSearchParams(queryString)
                    const currentAttempt = parseInt(urlParams.get('attempt')) + 1

                    params.photo = `${urlSplitted[0]}?attempt=${currentAttempt}`
                    row.params = JSON.stringify(params)
                } else if (decoded.parameters && decoded.parameters.migrate_to_chat_id) {

                    // group was updated to a supergroup chat
                    row.chat_id = decoded.parameters.migrate_to_chat_id
                    const params = JSON.parse(row.params)
                    params.chat_id = decoded.parameters.migrate_to_chat_id
                    row.params = JSON.stringify(params)
                } else {
                    row.status = -1
                    row.error = `${decoded.error_code}: ${decoded.description}`
                    if (!decoded.description.includes('chat not found')) {
                        await helpers.sendErrorToGroup({message: `\nHOST: ${row.host}\nMETHOD: ${row.method}\nERROR: ${decoded.error_code}: ${decoded.description}`})
                    }
                }
            } else {
                row.status = -1
                row.error = `${decoded.error_code}: ${decoded.description}`
            }
        }
        row.update = helpers.toSqlDateString(new Date())
        return await db.updateRequest(db.requestsTableName, id, row)
    })

    await Promise.all(dbWritePromises)
}

const processNextHostMessages = async () => {
    const result = await db.getBulkStatusChanges(db.bulkStatusChangeTableName)

    if (result === false) {return}

    const datas = []
    const responsePromises = result.rows.map(async row => {
        try {
            const data = JSON.parse(row.data)
            datas.push(data)
            const responsePromise = await fetch(`${row.host}/orders/orders/inoutReporterMessages`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({data: data, status: row.transition_status})
            })
            return responsePromise
        } catch (error) {
            return Promise.resolve({fetchFailed: true, error: error})
        }
    })

    const responses = await Promise.all(responsePromises)

    const decodedPromises = responses.map(async response => {
        if (!response.fetchFailed) {
            return await response.json()
        } else {
            Promise.resolve({ok: false, error_code: 500, description: response.error.message})
        }
    })

    const decodedRows = await Promise.all(decodedPromises)

    const dbWritePromises = decodedRows.map(async (decoded, index) => {
        const {id, ...row} = result.rows[index]
        
        if (decoded.ok && decoded.response && decoded.token) {
            row.status = 1
            
            const requests = helpers.generateTelegramApiRequests(decoded.response, decoded.token, row.host)
            
            if (requests.length > 0) {
                requests.map(async (tgApiRequestObj, index) => {
                    return await db.addRequest(db.requestsTableName, tgApiRequestObj)
                })
                await Promise.all(requests)
            }
        } else {
            row.status = -1
            row.error = decoded.error_code && decoded.description
                ? `${decoded.error_code}: ${decoded.description}`
                : 'Did not received success message, assuming as failure'
        }

        row.update = helpers.toSqlDateString(new Date())
        return await db.updateRequest(db.bulkStatusChangeTableName, id, row)
    })

    await Promise.all(dbWritePromises)
}

const runClearDBSchedule = async () => {

    const rule = new schedule.RecurrenceRule()
    rule.hour = 0
    rule.minute = 0
    rule.second = 0

    schedule.scheduleJob(rule, async () => {
        try {
            await db.clearDatabase(db.requestsTableName)
            await db.clearDatabase(db.bulkStatusChangeTableName)
        } catch (error) {
            await helpers.sendErrorToGroup(error)
        }
    })
}

const runRequestHandler = () => {
    setTimeout(async () => {
        
        try {
            await processNextQueue()
        } catch (error) {
            await helpers.sendErrorToGroup(error)
        }
        runRequestHandler()
    }, 3000)
}

const runChangedOrderStatusesRequestHandler = async () => {
    await processNextHostMessages()
    runChangedOrderStatusesRequestHandler()
}

module.exports = {runRequestHandler, runChangedOrderStatusesRequestHandler, runClearDBSchedule}