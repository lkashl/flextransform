const dayjs = require('dayjs');
const fs = require('fs');
const split2 = require('split2');

const By = require('./types/By');
const Aggregation = require('./types/Aggregation');
const Window = require('./types/Window')

// These globals allow us to write functions from the HTML page directly without needing to stringify
class google { }
const document = {}

const keyFromEvent = (event, bys) => bys.map(i => event[i.bySplit]).join('|')

const _sort = (order, data, ...keys) => {
    return data.sort((a, b) => {
        let directive = 0;
        keys.some(key => {
            const type = typeof a[key];

            if (type === 'number') {
                directive = order === 'asc' ? a[key] - b[key] : b[key] - a[key];
            } else if (type === 'string') {
                directive = order === 'asc' ? a[key].localeCompare(b[key]) : b[key].localeCompare(a[key]);
            }

            if (directive !== 0) return true;
        })

        return directive;
    })
}

class Transformer {

    constructor() {
        this.events = [];
        this.graphFlags = { trellis: false, trellisName: null }
        this.visualisations = [];
        this.checkpoints = {}
    }

    eval(modifier) {
        this.events.forEach(event => {
            const vals = modifier(event)
            if (vals) Object.assign(event, vals)
        })
        return this;
    }

    table(modifier) {
        this.events = this.events.map(event => {
            const vals = modifier(event)
            return vals;
        })
        return this;
    }

    rename(...entities) {
        this.events.forEach(event => {
            entities.forEach(([from, to]) => {
                event[to] = event[from]
                delete event[from]
            })
        })
        return this;
    }

    parseTime(value, customFormat) {
        this.events.forEach(event => {
            event[value] = dayjs(event[value], customFormat).valueOf()
        })
        return this;
    }

    bin(value, span) {
        this.events.forEach(event => {
            event[value] = Math.floor(event[value] / span) * span
        })
        return this;
    }

    fileScan(directory) {
        const items = fs.readdirSync(directory)
        this.events = items.map(item => {
            return {
                _fileInput: item
            }
        })
        return this;
    }

    async fileLoad(delim, parser) {
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise(resolve => {
                fs.createReadStream('./testData/' + obj._fileInput)
                    .pipe(split2(delim))
                    .on('data', line => {
                        const event = parser(line)
                        if (event !== null) content.push(event)
                    })
                    .on('end', () => {
                        obj._raw = content;
                        resolve(this)
                    })
            })
        })

        await Promise.all(tasks)
        return this;
    }

    output() {
        console.log(this.events)
        return this;
    }

    flatten() {
        const arraySize = this.events.reduce((acc, obj) => acc + obj._raw.length, 0)
        let flattened = new Array(arraySize)
        let i = 0

        this.events.forEach(obj => {
            const raws = obj._raw
            delete obj._raw

            raws.forEach(event => {
                flattened[i++] = {
                    ...obj,
                    _raw: event,
                }
            })

        })
        this.events = flattened;
        return this;
    }

    _stats(args, events) {
        const by = args.filter(arg => arg instanceof By)
        const aggregations = args.filter(arg => arg instanceof Aggregation);
        const targetFields = [... new Set(aggregations.map(i => i.field))]

        const map = {}

        events.forEach(item => {
            const key = keyFromEvent(item, by)

            if (!map[key]) {
                map[key] = {
                    _statsRaw: {},
                }

                // Add key fields
                by.forEach(i => {
                    map[key][i.bySplit] = item[i.bySplit]
                })
            }

            targetFields.forEach(field => {
                if (!map[key]._statsRaw[field]) map[key]._statsRaw[field] = [];
                const _values = map[key]._statsRaw[field];
                _values.push(item[field])
            })
        })

        const arr = Object.keys(map).map(key => {
            const result = map[key]

            aggregations.forEach(aggregation => {
                if (aggregation.sortable) map[key]._statsRaw[aggregation.field].sort((a, b) => a - b)

                const aggregationField = aggregation.outputField
                result[aggregationField] = aggregation.calculate(map[key])
            })

            delete map[key]._statsRaw
            return map[key]
        })

        return { arr, map, by, aggregations }
    }

    stats(...args) {
        this.events = this._stats(args, this.events).arr
        return this;
    }

    eventstats(...args) {
        const stats = this._stats(args, this.events)

        this.events.forEach(event => {
            const key = keyFromEvent(event, stats.by)

            Object.assign(event, stats.map[key])
        })

        return this
    }

    streamstats(...args) {
        const window = args.filter(i => i instanceof Window)
        const by = args.filter(i => i instanceof By)

        // Perform some validation
        if (window.length > 1) throw new Error('Only one window allowed in streamstats')
        if (window.length > 0 && by.length > 0) throw new Error('Window and By not supported together in streamstats')

        this.events.forEach((event, i) => {
            let start, byKey;
            if (window.length > 0) {
                start = Math.max(i - window[0].size + 1, 0)
                byKey = ""
            } else if (by.length > 0) {
                let backwardIndex = 0
                const thisKey = keyFromEvent(event, by)
                byKey = thisKey
                let keyChange = false
                while (!keyChange) {
                    const target = i - backwardIndex

                    if (target < 0) {
                        keyChange = true
                        break
                    }

                    const newKey = keyFromEvent(this.events[target], by)
                    if (thisKey !== newKey) {
                        keyChange = true
                        break
                    }

                    backwardIndex++
                }
                start = Math.max(i - backwardIndex + 1, 0)
            }

            const eventRange = this.events.slice(start, i + 1)
            Object.assign(event, this._stats(args, eventRange).map[byKey])
        })

        return this;
    }

    sort(order, ...keys) {
        this._events = _sort(order, this.events, keys)
        return this;
    }

    assert(funct) {
        const expect = (funct) => { if (!funct) throw new Error('Assertion failed') }
        this.events.forEach((event, i) => {
            funct(event, i, { expect })
        })
        return this;
    }

    build(name, type) {
        this.visualisations.push([name, type, this.events])
        return this;
    }

    checkpoint(operation, name) {

        const operations = {
            create: () => this.checkpoints[name] = this.events,
            retrieve: () => this.events = this.checkpoints[name],
            delete: () => delete this.checkpoints[name]
        }

        operations[operation]()
        return this;
    }

    mvexpand(target) {
        const arr = []
        this.events.forEach(event => {
            if (!event[target]) return arr.push(event)
            event[target].forEach((item) => {
                arr.push({
                    ...event,
                    [target]: item
                })
            })
        })

        this.events = arr
        return this;
    }

    toGraph(x, y, series, trellis, options = {}) {

        this.stats(
            new Aggregation(y, 'list', y),
            new Aggregation(series, 'list', series),
            new Aggregation(trellis, 'values', 'trellis'),
            new By(x), trellis ? new By(trellis) : null
        )

        const trellisMap = {}

        this.table(event => {
            const obj = {
                _time: event[x]
            }
            event[series].forEach((series, i) => obj[series] = event[y][i])

            if (trellis) {
                const tval = event[trellis][0]
                if (!trellisMap[tval]) trellisMap[tval] = []
                trellisMap[tval].push(obj)
            }

            return obj
        })

        if (trellis) {
            this.graphFlags.trellis = true;
            this.graphFlags.trellisName = Object.keys(trellisMap)
            this.events = Object.keys(trellisMap).map(tval => trellisMap[tval])
        }

        Object.assign(this.graphFlags, options)
        return this;
    }

    render() {
        const createElement = (name, type, eventData, { trellis, y2, sortX, trellisName, y2Type, y1Type, stacked }) => {
            if (!trellis) eventData = [eventData]

            let pairs = trellisName.map((name, i) => [name, eventData[i]]);
            pairs = pairs.sort((a, b) => a[0].localeCompare(b[0]))

            // Unzip back into separate arrays
            trellisName = pairs.map(p => p[0]);
            eventData = pairs.map(p => p[1]);

            eventData.forEach((trellis, i) => {
                const data = new google.visualization.DataTable();

                const series = {}, axis0 = { targetAxisIndex: 0 }, axis1 = { targetAxisIndex: 1 }

                if (y1Type) axis0.type = y1Type
                if (y2Type) axis1.type = y2Type

                // Create columns
                const columns = Object.keys(trellis[0])
                columns.forEach((key, i) => {
                    data.addColumn(typeof trellis[0][key], key)

                    if (y2 && i !== 0) {
                        let match = false;
                        if (y2 instanceof Array) { match = y2.includes(key) }
                        else if (y2 instanceof RegExp) { match = y2.test(key) }

                        if (match) series[i - 1] = axis1
                    }

                    if (!series[1 - i]) series[i - 1] = axis0
                })

                let rows = trellis.map(event => {
                    return columns.map(key => event[key])
                })

                rows = _sort(sortX, rows, 0)

                data.addRows(rows);

                const thisEntity = document.createElement('div')
                thisEntity.id = name
                document.body.appendChild(thisEntity)
                const chartElement = new google.visualization[type](thisEntity)
                google.visualization.events.addListener(chartElement, 'select', (e) => {
                    console.log(chartElement.getSelection()[1], chartElement.getSelection()[0])
                    tokens[name] = trellis[chartElement.getSelection()[0].row]
                    console.log(tokens[name])
                });

                const title = trellis ? name + trellisName[i] : name

                chartElement.draw(data, {
                    series, showRowNumber: false, legend: { position: 'bottom' }, title, isStacked: stacked
                })
            })
        }

        fs.writeFileSync('output.html', `
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['table', 'corechart']});
      google.charts.setOnLoadCallback(drawVis);
      const tokens = {}

      const _sort = ${_sort.toString()}
      const createElement = ${createElement.toString()}
      
      function drawVis() {
            ${this.visualisations.map(([name, type, data]) => {
            return `createElement('${name}', '${type}', ${JSON.stringify(data)}, ${JSON.stringify(this.graphFlags)})`
        })}
      }

    </script>
  </head>
  <body>
  </body>
</html>
        `)
    }
}

module.exports = { Transformer, Aggregation, By, Window }
