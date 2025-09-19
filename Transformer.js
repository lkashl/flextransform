const { assert } = require('console');
const dayjs = require('dayjs');
const fs = require('fs');
const split2 = require('split2')

class google {

}

const document = {}

class Aggregation {
    constructor(field, type, outputField = field, options) {
        this.type = type;
        this.field = field;
        this.outputField = outputField
        this.options = options;
        this.sortable = ['max', 'min', 'percentile', 'median'].includes(type)
    }

    count(values) {
        return values.length
    }

    distinctCount(values) {
        return new Set(values).size
    }

    list(values) {
        return values;
    }

    values(values) {
        return [...new Set(values)];
    }

    calculate(statObj) {
        return this[this.type](statObj._statsRaw[this.field])
    }

    max(values) {
        return values[values.length - 1]
    }

    min(values) {
        return values[0]
    }

    percentile(values) {
        const index = Math.round(this.options / 100 * (values.length - 1));
        return values[index]
    }

    median(values) {
        const index = Math.floor((values.length - 1) / 2);
        return values[index]
    }

    sum(values) {
        return values.reduce((a, b) => a + b, 0)
    }
}

class By {
    constructor(bySplit) {
        this.bySplit = bySplit;
    }
}

class Window {
    constructor(size) {
        this.size = size;
    }
}


const keyFromEvent = (event, bys) => bys.map(i => event[i.bySplit]).join('|')
class Transformer {

    constructor() {
        this.events = [];
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
        this.events = this.events.sort((a, b) => {
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

    toGraph(x, y, series) {
        this.stats(
            new Aggregation(y, 'list', y),
            new Aggregation(series, 'list', series),
            new By(x)
        )

        this.table(event => {
            const obj = {
                _time: event[x]
            }
            event[series].forEach((series, i) => obj[series] = event[y][i])
            return obj
        })
        return this;
    }

    render() {
        const createElement = (name, type, eventData) => {
            const data = new google.visualization.DataTable();
            const columns = Object.keys(eventData[0])
            columns.map(key => data.addColumn(typeof eventData[0][key], key)).join('\n')

            const rows = eventData.map(event => {
                return columns.map(key => event[key])
            })

            data.addRows(rows);

            const thisEntity = document.createElement('div')
            thisEntity.id = name
            document.body.appendChild(thisEntity)
            const chartElement = new google.visualization[type](thisEntity)
            google.visualization.events.addListener(chartElement, 'select', (e) => {
                console.log(chartElement.getSelection()[1], chartElement.getSelection()[0])
                tokens[name] = eventData[chartElement.getSelection()[0].row]
                console.log(tokens[name])
            });
            chartElement.draw(data, { showRowNumber: false, legend: { position: 'bottom' }, title: name })
        }

        fs.writeFileSync('output.html', `
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['table', 'corechart']});
      google.charts.setOnLoadCallback(drawVis);
      const tokens = {}

      const createElement = ${createElement.toString()}
      
      function drawVis() {
            ${this.visualisations.map(([name, type, data]) => {
            return `createElement('${name}', '${type}', ${JSON.stringify(data)})`
        })}
      }

    </script>
  </head>
  <body>
    <div id="table_div"></div>
    <div id="chart"></div>
  </body>
</html>
        `)
    }
}

module.exports = { Transformer, Aggregation, By, Window }
