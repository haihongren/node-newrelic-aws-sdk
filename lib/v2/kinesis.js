/*
 * Copyright 2020 New Relic Corporation. All rights reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

'use strict'

module.exports = {
  name: 'kinesis',
  type: 'message',
  validate: (shim, AWS) => {
    if (!shim.isFunction(AWS.Kinesis)) {
      shim.logger.debug('Could not find KINESIS, not instrumenting.')
      return false
    }
    return true
  },
  instrument,
}

function instrument(shim, AWS) {
  shim.setLibrary('KINESIS')

  shim.wrapReturn(
    AWS,
    'Kinesis',
    function wrapKinesis(shim, original, name, kinesis) {
      shim.recordProduce(kinesis, ['putRecord', 'putRecords'], wrapPublish)
    }
  )
}

function wrapPublish(shim, original, name, args) {
  const transactionHandle = shim._agent.getTransaction()
  const result = insertDTHeaders(transactionHandle, args[0])
  shim.logger.debug('New Relic DT header inserted:', result)
  return {
    callback: shim.LAST,
    destinationName: getDestinationName(args[0]),
    destinationType: shim.TOPIC,
    opaque: true,
  }
}

function getDestinationName({ StreamName, PartitionKey }) {
  return StreamName || PartitionKey || 'KinesisStream'
}

function insertDTHeaders(transactionHandle, allArgs) {
  const parsedData = checkPayload(allArgs.Data || allArgs.Records[0].Data)
  if (parsedData) {
    const headersObject = {}
    transactionHandle.insertDistributedTraceHeaders(headersObject)
    parsedData._newrelic = headersObject
    const newData = JSON.stringify(parsedData)
    const byteSize = Buffer.byteLength(newData, 'ascii')
    if (byteSize >= 1048576) {
      shim.logger.error('Payload size over 1MB limit to insert trace header')
      return false
    }
    if (allArgs.Data) {
      allArgs.Data = newData
    } else {
      allArgs.Records[0].Data = newData
    }
    return true
  } else {
    shim.logger.error('Unable to parse payload, unable to insert trace header')
  }
  return false
}

function checkPayload(payload) {
  try {
    return JSON.parse(payload)
  } catch (e) {
    shim.logger.info('Not JSON string. Try Base64 encoding')
  }
  try {
    return JSON.parse(Buffer.from(payload, 'base64').toString('ascii'), true)
  } catch (e) {
    return null
  }
}
