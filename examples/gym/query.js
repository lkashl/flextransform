const { Vaporous, By, Aggregation, Window } = require("../../Vaporous")


const dataFolder = __dirname + '/exampleData'
const main = async () => {

    console.log('Starting')
    const vaporous = await new Vaporous()
        // Load folder and files
        .fileScan(dataFolder)
        .csvLoad(({ data }) => {
            const event = {
                'seconds': Number.parseFloat(data.seconds),
                'left_kgf': Number.parseFloat(data.left_kgf),
                'left_cm': Number.parseFloat(data.left_cm),
                'left_cm_per_s': Number.parseFloat(data.left_cm_per_s),
                'right_kgf': Number.parseFloat(data.right_kgf),
                'right_cm': Number.parseFloat(data.right_cm),
                'right_cm_per_s': Number.parseFloat(data.right_cm_per_s),
                'phase': Number.parseFloat(data.phase),

                temps: [
                    Number.parseFloat(data.temp0_C),
                    Number.parseFloat(data.temp1_C),
                    Number.parseFloat(data.temp2_C),
                    Number.parseFloat(data.temp3_C),
                    Number.parseFloat(data.temp4_C),
                    Number.parseFloat(data.temp5_C),
                    Number.parseFloat(data.temp6_C),
                    Number.parseFloat(data.temp7_C)
                ]
            }

            return event;
        })

    vaporous
        .flatten()
        .eval(event => ({ ...event._raw, _fileInput: event._fileInput.split('/').at(-1) }))
        .sort('asc', 'seconds')
        .checkpoint('create', 'mainDataSeries')

        // Create pollint interval graph
        .delta('seconds', 'pollingInterval')
        .bin('seconds', 1)
        .stats(new Aggregation('pollingInterval', 'percentile', 'pollingInterval', 95), new By('seconds'), new By('_fileInput'))
        .toGraph('seconds', 'pollingInterval', '_fileInput')
        .build('Machine polling', 'LineChart', {
            columns: 1,
            tab: 'Diagnostics'
        })

        // Create temperature graph
        .checkpoint('retrieve', 'mainDataSeries')
        .bin('seconds', 1)
        .mvexpand('temps')
        .stats(new Aggregation('temps', 'max', 'maxTemp'),
            new By('seconds'), new By('_fileInput'), new By('_mvExpand_temps')
        )
        .toGraph('seconds', 'maxTemp', '_fileInput', '_mvExpand_temps')
        .build('Temps', 'LineChart', {
            columns: 3,
            tab: 'Diagnostics'
        })

        .render()
}

main()



