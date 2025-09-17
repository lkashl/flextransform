const { Transformer, By, Aggregation, Window } = require("./Transformer")

new Transformer()
    // Load folder and files
    .fileScan('./testData')
    .fileLoad('\n', event => JSON.parse(event))
    .flatten()

    // Parse timestamps and make variables accessible
    .eval(event => event._raw)
    .parseTime('timestamp')
    .bin('timestamp', 10000)
    .sort('dsc', 'deviceId')

    // Create streamstats and eventstats 
    .streamstats(new Aggregation('temp', 'sum'), new Aggregation('temp', 'list'), new By('deviceId'))
    .rename(['_streamstats', '_streamstats_by'])
    .streamstats(new Aggregation('temp', 'sum'), new Aggregation('temp', 'list'), new Window(100))
    .eventstats(new Aggregation('temp', 'median'), new Aggregation('temp', 'count'), new Aggregation('temp', 'sum'), new By('deviceId'))

    .eval(event => ({ deviation: event.temp - event._eventstats.temp.median, computedMedian: event._eventstats.temp.median }))

    // Evaluate the accuracy of streamstats and eventstats
    .assert((event, i, { expect }) => {
        if (i === 0) {
            expect(event._streamstats.temp.list.length === 1)
            expect(event._streamstats_by.temp.list.length === 1)
            expect(event._streamstats.temp.sum === event._streamstats_by.temp.sum)
            expect(event._eventstats.temp.count === 100)
        } else if (i % 100 === 99) {
            expect(event._streamstats.temp.list.length === 100)
            expect(event._streamstats_by.temp.list.length === 100)
            expect(event._streamstats.temp.sum === event._streamstats_by.temp.sum)
            expect(event._streamstats.temp.sum === event._eventstats.temp.sum)
            expect(event._eventstats.temp.count === 100)
        } else if (i % 100 === 0) {
            expect(event._streamstats.temp.list.length === 100)
            expect(event._streamstats_by.temp.list.length === 1)
            expect(event._eventstats.temp.count === 100)
        }
    })
    .stats(
        new Aggregation('temp', 'median'),
        new By('deviceId'), new By('timestamp')
    )

    // Transformation logic to marshall into a table
    // This needs to be reviwed to make lighter as the user shouldn't need to define this level of logic in every instance
    .table(event => {
        return {
            timestamp: event._keys.timestamp,
            deviceId: event._keys.deviceId,
            temp: event._stats.temp.median
        }
    })
    .stats(
        new Aggregation('temp', 'list'), new Aggregation('deviceId', 'list'),
        new By('timestamp')
    )
    .output()
    .table(event => {
        const obj = {
            _time: event._keys.timestamp
        }
        event._stats.deviceId.list.forEach((device, i) => obj[device] = event._stats.temp.list[i])
        return obj
    })
    .sort('asc', '_time')
    .output()
    .render()

console.log('end')