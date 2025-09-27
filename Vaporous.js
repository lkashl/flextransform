#!/usr/bin/env node

const dayjs = require('dayjs');
const fs = require('fs');
const split2 = require('split2');

const By = require('./types/By');
const Aggregation = require('./types/Aggregation');
const Window = require('./types/Window')
const path = require('path')

const styles = fs.readFileSync(__dirname + '/styles.css')

const Papa = require('papaparse')

// These globals allow us to write functions from the HTML page directly without needing to stringify
class google { }
const document = {}

const keyFromEvent = (event, bys) => bys.map(i => event[i.bySplit]).join('|')

const _sort = (order, data, ...keys) => {
    return data.sort((a, b) => {
        let directive = 0;

        keys.some(key => {
            directive = typeof a[key] === 'number' ? a[key] - b[key] : a[key].localeCompare(b[key])
            if (order === 'dsc') directive = directive * -1
            if (directive !== 0) return true;
        })

        return directive;
    })
}

class Vaporous {

    constructor() {
        this.events = [];
        this.visualisations = [];
        this.visualisationData = []
        this.graphFlags = []
        this.tabs = []

        this.savedMethods = {}
        this.checkpoints = {}
    }

    method(operation, name, method) {
        const operations = {
            create: () => {
                this.savedMethods[name] = method
            },
            retrieve: () => {
                this.savedMethods[name](this)
            },
            delete: () => {
                delete this.savedMethods[name]
            }
        }


        operations[operation]()
        return this
    }

    filter(...args) {
        this.events = this.events.filter(...args)
        return this
    }

    append(entities) {
        this.events = this.events.concat(entities)
        return this;
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
                _fileInput: path.resolve(directory, item)
            }
        })
        return this;
    }

    async csvLoad(parser) {
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                const thisStream = fs.createReadStream(obj._fileInput)

                Papa.parse(thisStream, {
                    header: true,
                    skipEmptyLines: true,
                    step: (row) => {
                        try {
                            const event = parser(row)
                            if (event !== null) content.push(event)
                        } catch (err) {
                            reject(err)
                        }
                    },
                    complete: () => {
                        obj._raw = content
                        resolve(this)
                    }
                })
            })
        })

        await Promise.all(tasks)
        return this;
    }

    async fileLoad(delim, parser) {
        const tasks = this.events.map(obj => {
            const content = []

            return new Promise((resolve, reject) => {
                fs.createReadStream(obj._fileInput)
                    .pipe(split2(delim))
                    .on('data', line => {
                        try {
                            const event = parser(line)
                            if (event !== null) content.push(event)
                        } catch (err) {
                            throw err;
                        }

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
        const backwardIterate = (event, i, by, maxBoundary = 0) => {
            let backwardIndex = 0
            const thisKey = keyFromEvent(event, by)
            const byKey = thisKey

            while (true) {
                const target = i - backwardIndex

                if (target < 0 || target < maxBoundary) break

                const newKey = keyFromEvent(this.events[target], by)
                if (thisKey !== newKey) break
                backwardIndex++
            }

            return { byKey, start: i - backwardIndex + 1 }
        }


        const window = args.filter(i => i instanceof Window)
        const by = args.filter(i => i instanceof By)

        // Perform some validation
        if (window.length > 1) throw new Error('Only one window allowed in streamstats')

        this.events.forEach((event, i) => {
            let start, byKey = "";

            // Refine to window size 
            if (window.length > 0) start = Math.max(i - window[0].size + 1, 0)
            if (by.length !== 0) ({ start, byKey } = backwardIterate(event, i, by, start))

            const eventRange = this.events.slice(start, i + 1)
            Object.assign(event, this._stats(args, eventRange).map[byKey])
        })

        return this;
    }

    delta(field, remapField) {
        this.streamstats(new Aggregation(field, 'range', remapField), new Window(2))
        return this;
    }

    sort(order, ...keys) {
        this.events = _sort(order, this.events, ...keys)
        return this;
    }

    assert(funct) {
        const expect = (funct) => { if (!funct) throw new Error('Assertion failed') }
        this.events.forEach((event, i) => {
            funct(event, i, { expect })
        })
        return this;
    }

    build(name, type, { tab = 'Default', columns = 2 }) {

        const visualisationOptions = { tab, columns }

        const data = JSON.stringify(this.events)
        const lastData = this.visualisationData.at(-1)

        if (lastData !== data) this.visualisationData.push(data)
        this.visualisations.push([name, type, visualisationOptions, this.visualisationData.length - 1, this.graphFlags[this.graphFlags.length - 1]])

        if (visualisationOptions.tab && !this.tabs.includes(visualisationOptions.tab)) this.tabs.push(visualisationOptions.tab)

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
            event[target].forEach((item, i) => {
                arr.push({
                    ...event,
                    [target]: item,
                    [`_mvExpand_${target}`]: i
                })
            })
        })

        this.events = arr
        return this;
    }

    toGraph(x, y, series, trellis, options = {}) {

        if (!(y instanceof Array)) y = [y]
        if (options.y2 instanceof RegExp) options.y2 = options.y2.toString()

        const yAggregations = y.map(item => new Aggregation(item, 'list', item))

        this.stats(
            ...yAggregations,
            new Aggregation(series, 'list', series),
            new Aggregation(trellis, 'values', 'trellis'),
            new By(x), trellis ? new By(trellis) : null
        )

        const trellisMap = {}

        this.table(event => {
            const obj = {
                _time: event[x]
            }
            event[series].forEach((series, i) => {
                y.forEach(item => {
                    const name = y.length === 1 ? series : `${series}_${item}`
                    obj[name] = event[item][i]
                })
            })

            if (trellis) {
                const tval = event.trellis[0]
                if (!trellisMap[tval]) trellisMap[tval] = []
                trellisMap[tval].push(obj)
            }

            return obj
        })

        const graphFlags = {}
        if (trellis) {
            graphFlags.trellis = true;
            graphFlags.trellisName = Object.keys(trellisMap)
            this.events = Object.keys(trellisMap).map(tval => trellisMap[tval])
        }

        Object.assign(graphFlags, options)
        this.graphFlags.push(graphFlags)
        return this;
    }

    render() {
        const classSafe = (name) => name.replace(/[^a-zA-Z0-9]/g, "_")

        const createElement = (name, type, visualisationOptions, eventData, { trellis, y2, sortX, trellisName = "", y2Type, y1Type, stacked, y1Min, y2Min }) => {

            if (typeof y2 === 'string') {
                y2 = y2.split("/")
                const flags = y2.at(-1)
                y2.pop()
                const content = y2.splice(1).join("/")
                y2 = new RegExp(content, flags)
            }

            if (classSafe(visualisationOptions.tab) !== selectedTab) return;

            eventData = visualisationData[eventData]
            if (!trellis) eventData = [eventData]
            else {
                let pairs = trellisName.map((name, i) => [name, eventData[i]]);
                pairs = pairs.sort((a, b) => a[0].localeCompare(b[0]))

                // Unzip back into separate arrays
                trellisName = pairs.map(p => p[0]);
                eventData = pairs.map(p => p[1]);
            }

            eventData.forEach((trellisData, i) => {
                const data = new google.visualization.DataTable();

                const series = {}, axis0 = { targetAxisIndex: 0 }, axis1 = { targetAxisIndex: 1 }

                if (y1Type) axis0.type = y1Type
                if (y2Type) axis1.type = y2Type

                // Create columns
                const columns = Object.keys(trellisData[0])
                columns.forEach((key, i) => {
                    data.addColumn(typeof trellisData[0][key], key)

                    if (y2 && i !== 0) {
                        let match = false;
                        if (y2 instanceof Array) { match = y2.includes(key) }
                        else if (y2 instanceof RegExp) { match = y2.test(key) }

                        if (match) series[i - 1] = axis1
                    }

                    if (!series[i - 1]) series[i - 1] = axis0
                })

                let rows = trellisData.map(event => {
                    return columns.map(key => event[key])
                })

                rows = _sort(sortX, rows, 0)

                data.addRows(rows);

                const columnCount = visualisationOptions.columns || 2
                const thisEntity = document.createElement('div')
                thisEntity.className = "parentHolder"
                thisEntity.style = `flex: 1 0 calc(${100 / columns}% - 6px); max-width: calc(${100 / columnCount}% - 6px);`


                const thisGraph = document.createElement('div')
                thisGraph.className = "graphHolder"
                thisEntity.appendChild(thisGraph)
                document.getElementById('content').appendChild(thisEntity)

                const chartElement = new google.visualization[type](thisGraph)

                google.visualization.events.addListener(chartElement, 'select', (e) => {
                    console.log(chartElement.getSelection()[1], chartElement.getSelection()[0])
                    tokens[name] = trellisData[chartElement.getSelection()[0].row]
                    console.log(tokens[name])
                });

                const title = trellis ? name + trellisName[i] : name

                chartElement.draw(data, {
                    series, showRowNumber: false, legend: { position: 'bottom' }, title, isStacked: stacked,
                    width: document.body.scrollWidth / columnCount - (type === "LineChart" ? 12 : 24),
                    animation: { duration: 500, startup: true },
                    chartArea: { width: '85%', height: '75%' },
                    vAxis: {
                        viewWindow: {
                            min: y1Min
                        }
                    }
                })
            })
        }

        const filePath = './Vaporous_generation.html'
        fs.writeFileSync(filePath, `
<html>
        <head>
            <style>
                ${styles}
            </style>
    <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
    <script type="text/javascript">
      google.charts.load('current', {'packages':['table', 'corechart']});
      google.charts.setOnLoadCallback(drawVis);

      const classSafe = ${classSafe.toString()}

      var selectedTab = classSafe("${this.tabs[0]}")
      const tokens = {}
      const visualisationData = [${this.visualisationData.join(',')}]

      const _sort = ${_sort.toString()}
      const createElement = ${createElement.toString()}
      
      function drawVis(tab) {
            if (tab) {
                if (selectedTab) document.getElementById(selectedTab).classList.remove('selectedTab')
                selectedTab = tab
            } else if (${this.tabs.length > 0}) {
                selectedTab = classSafe('${this.tabs[0]}')
            }

            if (selectedTab) document.getElementById(selectedTab).classList.add('selectedTab')
            document.getElementById('content').innerHTML = ''
            ${this.visualisations.map(([name, type, visualisationOptions, dataIndex, graphFlags]) => {
            return `createElement('${name}', '${type}', ${JSON.stringify(visualisationOptions)} ,${dataIndex}, ${JSON.stringify(graphFlags)})`
        })}
      }

    </script>
  </head>
  <body>
        ${this.tabs.length > 0 ? `<div class='tabBar'>
            ${this.tabs.map(tab => `<div id=${classSafe(tab)} class='tabs' onclick="drawVis('${classSafe(tab)}')">${tab}</div>`).join("\n")}
        </div>` : ''}

    <div id='content'>
        </div>
  </body>
</html>
        `)

        console.log('File ouput created ', path.resolve(filePath))
    }
}

module.exports = { Vaporous, Aggregation, By, Window }
