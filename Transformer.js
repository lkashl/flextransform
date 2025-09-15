const fs = require('fs')

class Aggregation {
    constructor(field, type, options) {
        this.type = type;
        this.field = field;
        this.options = options;
        this.sortable = ['max', 'min', 'percentile'].includes(type)
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
        return values;
    }

    calculate(statObj) {
        return this[this.type](statObj._stats[this.field])
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
}

class By {
    constructor(bySplit) {
        this.bySplit = bySplit;
    }
}


class Transformer {
    constructor() {
        this.events = [];
    }

    fileScan(directory) {
        const items = fs.readdirSync(directory)
        this.events = items.map(item => {
            return {
                _raw: item
            }
        })
        return this;
    }

    fileLoad(delim, parser) {
        this.events = this.events.map(({ _raw }) => {
            const data = fs.readFileSync('./testData/' + _raw, 'utf-8').toString().split(delim).map(event => parser(event))
            return data;
        })

        return this;
    }

    output() {
        console.log(this.events)
        return this;
    }

    flatten() {
        this.events = this.events.flat();
        return this;
    }

    stats(...args) {
        const by = [], aggregations = [];

        args.forEach(arg => {
            if (arg instanceof By) {
                by.push(arg.bySplit)
            } else if (arg instanceof Aggregation) {
                aggregations.push(arg)
            }
        })

        const map = {}

        this.events.forEach(item => {
            const key = by.map(i => item[i]).join('|')

            if (!map[key]) {
                map[key] = {
                    _keys: {},
                    _stats: {},
                    _statsProcessed: {}
                }

                // Add key fields
                by.forEach(i => {
                    map[key]._keys[i] = item[i]
                })
            }

            aggregations.forEach(agg => {
                if (!map[key]._stats[agg.field]) map[key]._stats[agg.field] = [];

                const _values = map[key]._stats[agg.field];
                _values.push(item[agg.field])
            })
        })

        this.events = Object.keys(map).map(key => {
            const result = map[key]._statsProcessed

            aggregations.forEach(aggregation => {
                if (!result[aggregation.field]) result[aggregation.field] = {}
                if (aggregation.sortable) map[key]._stats[aggregation.field].sort((a, b) => a - b)
                result[aggregation.field][aggregation.type] = aggregation.calculate(map[key])
            })

            return map[key]
        })

        return this;
    }
}



new Transformer()
    .fileScan('./testData')
    .output()
    .fileLoad('\n', event => JSON.parse(event))
    .output()
    .flatten()
    .output()
    .stats(new Aggregation('temp', 'percentile', 90), new By('deviceId'))
    .output()

console.log('end')