const files = 10;
const observations = 100;

const fs = require('fs')

const deviceAlias = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Heidi', 'Ivan', 'Judy'];

const temperatureEvent = (i) => {
    return {
        temp: Math.floor(Math.random() * 100),
        timestamp: new Date().toISOString(),
        deviceId: deviceAlias[i % deviceAlias.length]
    }
}

for (let i = 0; i < files; i++) {
    const name = `temp_${i.toString().padStart(1, '0')}_10.jsonStream`

    const lines = []
    for (let i = 0; i < observations; i++) {
        lines.push(JSON.stringify(temperatureEvent(i)))
    }

    fs.writeFileSync('./testData/' + name, lines.join('\n'))
}