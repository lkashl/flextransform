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

module.exports = Aggregation