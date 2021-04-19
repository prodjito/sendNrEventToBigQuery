exports.handler = async (event, context) => {

    const {BigQuery} = require('@google-cloud/bigquery')

    //the bigquery client that will be used to insert records into BigQuery tables
    const bigquery = new BigQuery()

    //each NR event has an event type.
    //as the lambda processes NR events, it will populate this map
    //which is later used to retrieve all the events given a particular event type
    let eventTypeToTableRowsMap = new Map()

    //the events that will be written into S3 for back up
    //not to be confused with the events that will be inserted into BigQuery
    let outputToS3 = new Array()

    for (const record of event.records) {
        //console.log(record.data)

        let outputRecord = {
            recordId : record.recordId,
            result: 'Ok', //this is changed to ProcessingFailed later if JSON parsing fails
            data: record.data
        }
        outputToS3.push(outputRecord)

        try{
            // Kinesis data is base64 encoded so decode here
            let recordData = Buffer.from(record.data, 'base64').toString()
            //console.log('recordData: ' + recordData)
            var events = recordData.split('\n').filter(Boolean);//a record can contain multple NR events
            for (const eventStr of events) {
                //console.log('split: ' + eventStr)
                let event = JSON.parse(eventStr)
                let tableRows;
                if(eventTypeToTableRowsMap.has(event.eventType)){
                    tableRows = eventTypeToTableRowsMap.get(event.eventType)
                }else{
                    tableRows = new Array()
                    eventTypeToTableRowsMap.set(event.eventType, tableRows)
                }
                tableRows.push(event)
            }

        } catch (e) {
            console.log(e)
            outputRecord.result = 'ProcessingFailed'
        }
    }

    const options = {ignoreUnknownValues : true}

    for (const [eventType, tableRows] of eventTypeToTableRowsMap) {
        await bigquery
          .dataset('NewRelic')
          .table(eventType) //assume there is a BigQuery table with the same name as the event type
          .insert(tableRows, options)
        console.log(`Inserted ${tableRows.length} ${eventType} rows in BigQuery`)
    }

    return { records: outputToS3 }
};
