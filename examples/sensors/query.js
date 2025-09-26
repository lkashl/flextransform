const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")
const { generateData, dataFolder } = require("./sensor_data")

generateData()

const main = async () => {

    const sensors = await new Vaporous()
        // Load folder and files
        .fileScan(dataFolder)
        .fileLoad('\n', event => JSON.parse(event))

    sensors
        .flatten()

        // Parse timestamps and make variables accessible
        .eval(event => event._raw)
        .parseTime('timestamp')
        .bin('timestamp', 60 * 60 * 1000)
        .sort('asc', 'deviceId', 'timestamp')
        .streamstats(
            new Aggregation('totalEnergySpend', 'range', 'energySpend'),
            new Window(2), new By('deviceId')
        )
        .streamstats(
            new Aggregation('temp', 'sum', 'streamBySum'),
            new Aggregation('temp', 'list', 'streamByList'),
            new By('deviceId')
        )
        .streamstats(
            new Aggregation('temp', 'sum', 'streamWindowSum'),
            new Aggregation('temp', 'list', 'streamWindowList'),
            new Window(100)
        )
        .eventstats(
            new Aggregation('temp', 'median', 'eventMedian'),
            new Aggregation('temp', 'count', 'eventCount'),
            new Aggregation('temp', 'sum', 'eventSum'),
            new By('deviceId')
        )
        // Evaluate the accuracy of streamstats and eventstats
        .assert((event, i, { expect }) => {
            if (i === 0) {
                expect(event.streamWindowList.length === 1)
                expect(event.streamByList.length === 1)
                expect(event.streamBySum === event.streamWindowSum)
                expect(event.eventCount === 100)
            } else if (i % 100 === 99) {
                expect(event.streamWindowList.length === 100)
                expect(event.streamByList.length === 100)
                expect(Math.floor(event.streamWindowSum) === Math.floor(event.streamBySum))
                expect(Math.floor(event.streamWindowSum) === Math.floor(event.eventSum))
                expect(event.eventCount === 100)
            } else if (i % 100 === 0) {
                expect(event.streamWindowList.length === 100)
                expect(event.streamByList.length === 1)
                expect(event.eventCount === 100)
            }
        })
        .table(event => ({
            deviation: Math.round((event.temp - event.eventMedian) / event.eventMedian * 100),
            median: event.eventMedian,
            temp: event.temp,
            time: event.timestamp,
            deviceId: event.deviceId,
            firmware: event.firmware,
            energySpend: event.energySpend,
            variant: ['sensora', 'sensorb']
        }))
        .mvexpand('variant')
        .eval(event => {
            if (event.variant === 'sensorb') event.temp = event.temp - 6
        })
        .checkpoint('create', 'temperatureData')
        .toGraph('time', ['energySpend', 'temp'], 'variant', 'deviceId', {
            y1Type: 'bars',
            stacked: true,
            y2Type: 'lines',
            y2: /.+_temp/g,
            sortX: 'asc'
        })
        .build('Raw Temp', 'Table', {
            tab: "Example Table",
            columns: 3
        })
        .build('Temp and energy - Device: ', 'LineChart', {
            tab: "Example Graphs",
            columns: 3
        })

        .render()
}

main()