const files = 10;
const observations = 100;

const fs = require('fs');
const path = require('path');

const deviceAlias = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy'];
const deviceFirmware = ['v0', 'v1', 'v2']
const dataFolder = __dirname + '/exampleData'

const jitter = (base, variance) => {
    return base + (Math.random() * variance * 2) - variance
}

const now = new Date().valueOf()

const second = 1000
const min = second * 60
const hour = min * 60

const sensorEvent = (i) => {
    const multiplier = Math.floor(i / deviceAlias.length)
    return {
        deviceFirmware: deviceFirmware[i % deviceFirmware.length],
        deviceId: deviceAlias[i % deviceAlias.length],

        temp: jitter(12, 15),
        humidity: jitter(50, 50),
        totalEnergySpend: multiplier * 100 + jitter(50, 50),

        timestamp: new Date(now + multiplier * hour + jitter(30 * min, 15 * min)).toISOString(),
    }
}



const generateData = () => {
    try {
        fs.mkdirSync(dataFolder)
    } catch (err) {
        if (err.code != "EEXIST") throw err;
        return
    }

    for (let i = 0; i < files; i++) {
        const name = `temp_${i.toString().padStart(1, '0')}_10.jsonStream`

        const lines = []
        for (let j = 0; j < observations; j++) {
            lines.push(JSON.stringify(sensorEvent(i * observations + j)))
        }

        fs.writeFileSync(path.resolve(dataFolder, name), lines.join('\n'))
    }
}

module.exports = {
    sensorEvent,
    generateData,
    dataFolder
}