const os = require('os')

function toSqlDateString(date) {
    const year = date.getFullYear()
    const month = date.getMonth() + 1
    const day = date.getDate()

    const hour = date.getHours()
    const minute = date.getMinutes()
    const second = date.getSeconds()

    const fHour = hour >= 10 ? hour : `0${hour}`
    const fMinute = minute >= 10 ? minute : `0${minute}`
    const fSecond = second >= 10 ? second : `0${second}`
    const fDay = day >= 10 ? day : `0${day}`
    const fMonth = month >= 10 ? month : `0${month}`

    return `${year}-${fMonth}-${fDay} ${fHour}:${fMinute}:${fSecond}`
}

async function sendErrorToGroup(err, origin='') {
    let error2 = null
    try {
        const apiUrl = 'https://api.telegram.org/bot5849831361:AAFsY3mWq1S7cSDQxQgBBH9J3QG92PMDJVY/'
        const groupID = -1003449990258
        
        const resp = await fetch(`${apiUrl}sendMessage`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                chat_id: groupID,
                text: 
                    `<b>${String(os.hostname())}</b>\nCaught exception: ${err.message}\nException origin: ${origin}`,
                parse_mode: 'HTML' 
            })
        })
    } catch (error) {
        try {
            const apiUrl = 'https://api.telegram.org/bot8391939404:AAF6oVi6wdVsWzlLAKVIt1aZJ5P-9X4gz2s/'
            const chatID = 7976312976
            
            const resp = await fetch(`${apiUrl}sendMessage`, {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    chat_id: chatID,
                    text: 
                        `<b>${String(os.hostname())}</b>\nCaught exception: ${err.message}\nException origin: ${origin}`,
                    parse_mode: 'HTML' 
                })
            })
        } catch (errorInner) {
            error2 = errorInner
        }
    } finally {
        console.error(error2)
    }
}

function generateTelegramApiRequests(data, token, host) {

    const rows = []

    data.forEach(obj => {
        if (!obj.message || !obj.chatIds || obj.chatIds.length == 0) {return}
        const message = obj.message
        const chatIds = obj.chatIds

        chatIds.forEach(chatId => {

            const params = {
                chat_id: chatId,
                text: message,
                parse_mode: 'HTML'
            }

            if (obj.menu && Object.values(obj.menu).length > 0) {
                params['reply_markup'] = obj.menu
            }

            rows.push({
                chat_id: chatId,
                url: `https://api.telegram.org/bot${token}/`,
                method: 'sendMessage',
                host: host,
                params: JSON.stringify(params),
                status: 0,
                time: toSqlDateString(new Date()),
                token: token
            })
        })
    })
    

    return rows
}

module.exports = {toSqlDateString, sendErrorToGroup, generateTelegramApiRequests}