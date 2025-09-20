const { Transformer, By, Aggregation, Window } = require("./Transformer")

const main = async () => {

    const fileContents = await new Transformer()
        // Load folder and files
        .fileScan('./testData')
        .fileLoad('\n', event => JSON.parse(event))

    fileContents
        .flatten()

        // Parse timestamps and make variables accessible
        .eval(event => event._raw)
        .parseTime('timestamp')
        .bin('timestamp', 10000)
        .sort('dsc', 'deviceId')

        // Create streamstats and eventstats 
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
                expect(event.streamWindowSum === event.streamBySum)
                expect(event.streamWindowSum === event.eventSum)
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
            variant: ['sensora', 'sensorb', 'sensorc']
        }))
        .output()
        .mvexpand('variant')
        .eval(event => {
            if (event.variant === 'sensorb') event.temp = event.temp - 1
        })
        .output()
        .checkpoint('create', 'temperatureData')
        .toGraph('time', 'temp', 'variant', 'deviceId', {
            y1Type: 'bars',
            stacked: true,
            y2Type: 'lines',
            y2: ['sensorb'],
            sortX: 'asc'
        })
        .build('Raw Temp', 'Table')
        .build('Temp by device - ', 'LineChart')

        // .checkpoint('retrieve', 'temperatureData')
        // .toGraph('time', 'deviation', 'deviceId')
        // .build('Offset temp', 'Table')
        // .build('Offset temp', 'LineChart')
        // .sort('asc', '_time')

        .render()
}

main()