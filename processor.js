const db = require('./db.js')
const helpers = require('./helpers.js')
const info = require('./info.js')
const schedule = require('node-schedule')

const { File } = require('node:buffer');
if (!global.File) { global.File = File; }

const { ProxyAgent } = require('undici');

const processNextQueue = async () => {
    const result = await db.getRequests(db.requestsTableName);
    if (!result || result.rows.length === 0) return;

    try {
        const CHUNK_SIZE = 10; 
        const rows = result.rows;
        const allChunkPromises = [];

        // 1. Group rows into chunks and assign a proxy to each group
        for (let i = 0; i < rows.length; i += CHUNK_SIZE) {
            const chunk = rows.slice(i, i + CHUNK_SIZE);
            
            // Assign a unique proxy for this specific batch of 10
            const chunkProxyAgent = new ProxyAgent(info.getRandomProxyUrl());

            // 2. Map this chunk into a "Batch Promise"
            const chunkPromise = Promise.all(chunk.map(async (row) => {
                let decoded = null;

                try {
                    const response = await fetch(`${row.url}${row.method}`, {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: row.params,
                        dispatcher: chunkProxyAgent 
                    });
                    decoded = await response.json();
                } catch (error) {
                    decoded = { ok: false, error_code: 500, description: error.message };
                }

                // --- Database Logic Start ---
                const { id, ...rowUpdate } = row;

                if (decoded.ok) {
                    rowUpdate.status = 1;
                } else {
                    if (decoded.error_code == 400) {
                        const params = JSON.parse(rowUpdate.params);
                        if (params.media) {
                            let skipMedia = false;
                            params.media.forEach((media, index) => {
                                if (skipMedia) return;
                                const [urlBase, queryString] = media.media.split('?');
                                const urlParams = new URLSearchParams(queryString || 'attempt=0');
                                const currentAttempt = (parseInt(urlParams.get('attempt')) || 0) + 1;

                                if (currentAttempt >= 3) {
                                    rowUpdate.status = -1;
                                    rowUpdate.error = `${decoded.error_code}: ${decoded.description}`;
                                    skipMedia = true;
                                } else {
                                    params.media[index].media = `${urlBase}?attempt=${currentAttempt}`;
                                }
                            });
                            rowUpdate.params = JSON.stringify(params);
                        } else if (params.photo) {
                            const [urlBase, queryString] = params.photo.split('?');
                            const urlParams = new URLSearchParams(queryString || 'attempt=0');
                            const currentAttempt = (parseInt(urlParams.get('attempt')) || 0) + 1;
                            params.photo = `${urlBase}?attempt=${currentAttempt}`;
                            rowUpdate.params = JSON.stringify(params);
                        }

                        if (decoded.parameters && decoded.parameters.migrate_to_chat_id) {
                            rowUpdate.chat_id = decoded.parameters.migrate_to_chat_id;
                            params.chat_id = decoded.parameters.migrate_to_chat_id;
                            rowUpdate.params = JSON.stringify(params);
                        } else if (rowUpdate.status !== -1) {
                            rowUpdate.status = -2;
                            rowUpdate.error = `${decoded.error_code}: ${decoded.description}`;
                        }
                    } else {
                        rowUpdate.status = -1;
                        rowUpdate.error = `${decoded.error_code}: ${decoded.description}`;
                    }
                }

                rowUpdate.update = helpers.toSqlDateString(new Date());
                await db.updateRequest(db.requestsTableName, id, rowUpdate);
                // --- Database Logic End ---
            }));

            allChunkPromises.push(chunkPromise);
        }

        // 3. Execute all chunks (and all proxies) simultaneously
        await Promise.all(allChunkPromises);

    } catch (error) {
        await helpers.sendErrorToGroup(error, "processor.js -> processNextQueue()");
    }
}

const processNextHostMessages = async () => {
    const result = await db.getBulkStatusChanges(db.bulkStatusChangeTableName);
    if (!result || result.rows.length === 0) return;

    try {
        for (const row of result.rows) {
            let decoded = null;
            const { id, ...rowUpdate } = row;

            try {
                const data = JSON.parse(row.data);
                const response = await fetch(`${row.host}/api/order/inoutReporterMessages`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ data: data, status: row.transition_status })
                });
                decoded = await response.json();
            } catch (error) {
                decoded = { ok: false, error_code: 500, description: error.message };
            }

            if (decoded && decoded.ok && decoded.response && decoded.token) {
                rowUpdate.status = 1;
                const requests = helpers.generateTelegramApiRequests(decoded.response, decoded.token, row.host);

                if (requests.length > 0) {
                    // Sequential DB insertion
                    for (const tgReq of requests) {
                        await db.addRequest(db.requestsTableName, tgReq);
                    }
                }
            } else {
                rowUpdate.status = -1;
                rowUpdate.error = decoded?.description || 'Host response failed';
            }

            rowUpdate.update = helpers.toSqlDateString(new Date());
            await db.updateRequest(db.bulkStatusChangeTableName, id, rowUpdate);
        }
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'processor.js -> processNextHostMessages()');
    }
}

const processNextDisabledChats = async () => {
    const result = await db.getDisabledChatRequests(db.requestsTableName)
    try {
        if (result === false) {return}
        const groupedByHost = result.rows.reduce((acc, row) => {
            const params = JSON.parse(row.params)
            const messageThreadId = params['message_thread_id'] ?? null
            if (!acc[row.host]) {acc[row.host] = []}
            if (!acc[row.host][row.chat_id]) {acc[row.host][row.chat_id] = {}}
            if (messageThreadId) {
                acc[row.host][row.chat_id][messageThreadId] = null
            }
            return acc
        }, {})

        const responsePromises = Object.keys(groupedByHost).map(async host => {
            const entries = Object.keys(groupedByHost[host]).reduce((acc, chatId) => {
                if (Object.keys(groupedByHost[host][chatId]).length) {
                    acc = [...acc, ...Object.keys(groupedByHost[host][chatId]).map(messageThreadId => ({chat_id: chatId, message_thread_id: messageThreadId}))]
                } else {
                    acc.push({chat_id: chatId})
                }
                return acc
            }, [])
            try {
                const responsePromise = await fetch(`https://${host}/api/disabledChats`, {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({entries: entries})
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
            if (decoded.success) {
                row.status = 2
                row.update = helpers.toSqlDateString(new Date())
                return await db.updateRequest(db.requestsTableName, id, row)
            }
            return Promise.resolve(true)
        })
        await Promise.all(dbWritePromises)
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'processor.js -> processNextDisabledChats()')
    }
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
            await helpers.sendErrorToGroup(error, 'processor.js -> scheduleJob()')
        }
    })
}

const runRequestHandler = () => {
    setTimeout(async () => {
        try {
            await processNextQueue();
        } catch (error) {
            await helpers.sendErrorToGroup(error, 'processor.js -> runRequestHandler()');
        }
        runRequestHandler();
    }, 1000); // Check for new unique recipients every 0.5 seconds
}

const runChangedOrderStatusesRequestHandler = async () => {
    try {
        await processNextHostMessages();
    } catch (e) { console.error(e); }
    setTimeout(runChangedOrderStatusesRequestHandler, 5000);
}

const runDisabledChatsHandler = async () => {
    try {
        await processNextDisabledChats();
    } catch (error) {
        await helpers.sendErrorToGroup(error, 'processor.js -> runDisabledChatsHandler()');
    }
    setTimeout(runDisabledChatsHandler, 10000);
}

module.exports = {runRequestHandler, runChangedOrderStatusesRequestHandler, runClearDBSchedule, runDisabledChatsHandler}