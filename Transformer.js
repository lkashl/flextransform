const { assert } = require('console');
const dayjs = require('dayjs');
const fs = require('fs')

class Aggregation {
    constructor(field, type, options) {
        this.type = type;
        this.field = field;
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

    fileLoad(delim, parser) {
        this.events.forEach((obj) => {
            const data = { _raw: fs.readFileSync('./testData/' + obj._fileInput, 'utf-8').toString().split(delim).map(event => parser(event)) }
            Object.assign(obj, data);
        })

        return this;
    }

    output() {
        console.log(this.events)
        return this;
    }

    flatten() {
        let flattened = []
        this.events.forEach(obj => {
            obj._raw.forEach(event => {
                flattened.push({
                    ...obj,
                    _raw: event,
                })
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
                    _keys: {},
                    _stats: {},
                    _statsRaw: {},
                }

                // Add key fields
                by.forEach(i => {
                    map[key]._keys[i.bySplit] = item[i.bySplit]
                })
            }

            targetFields.forEach(field => {
                if (!map[key]._statsRaw[field]) map[key]._statsRaw[field] = [];
                const _values = map[key]._statsRaw[field];
                _values.push(item[field])
            })
        })



        const arr = Object.keys(map).map(key => {
            const result = map[key]._stats

            aggregations.forEach(aggregation => {
                if (!result[aggregation.field]) result[aggregation.field] = {}
                if (aggregation.sortable) map[key]._statsRaw[aggregation.field].sort((a, b) => a - b)
                result[aggregation.field][aggregation.type] = aggregation.calculate(map[key])
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

            event._eventstats = stats.map[key]._stats
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
            event._streamstats = this._stats(args, eventRange).map[byKey]?._stats
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
    render() {
        const columns = Object.keys(this.events[0])
        const columnCode = columns.map(key => `data.addColumn('${typeof this.events[0][key]}', '${key}')`).join('\n')
        const rows = this.events.map(event => {
            return columns.map(key => event[key])
        })
        const rowCode = `data.addRows(${JSON.stringify(rows)});`

        const table = `        
        var table = new google.visualization.Table(document.getElementById('table_div'));
        table.draw(data, {showRowNumber: false, width: '80%', height: '60%'});
        `

        const line = `
        var options = {
          title: 'Company Performance',
          legend: { position: 'bottom' }
        };

        var chart = new google.visualization.LineChart(document.getElementById('chart'));
        chart.draw(data, options);
        `
        fs.writeFileSync('output.html', `
<html>
  <head>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['table', 'corechart']});
      google.charts.setOnLoadCallback(drawVis);

      function drawVis() {
        var data = new google.visualization.DataTable();
        ${columnCode}
        ${rowCode}
        
        ${line}
        ${table}

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
