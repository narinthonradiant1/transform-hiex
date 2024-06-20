const fs = require('fs')
const csv = require('csv-parser')
const createCsvWriter = require('csv-writer').createObjectCsvWriter
const dayjs = require('dayjs')

const hotelId = 3297

const roomTypes = {
  OSTN: 8073,
  OWCN: 8074,
  OAHN: 8075,
  OSUN: 8076,
  OADN: 8077,
  OAAN: 8078,
  TSTN: 8079,
  PF: 8252,
  CSTN: 8254
}

const marketSegments = {
  A: 157,
  B: 158,
  C: 163,
  D: 176,
  F: 173,
  G: 160,
  H: 166,
  I: 167,
  J: 170,
  K: 172,
  L: 162,
  M: 164,
  N: 159,
  O: 156,
  P: 161,
  Q: 165,
  R: 175,
  S: 171,
  T: 174,
  U: 168,
  V: 155,
  W: 169,
  Y: 177
}

const readCSV = (filePath) => {
  return new Promise((resolve, reject) => {
    const rows = []
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', (data) => rows.push(data))
      .on('end', () => resolve(rows))
      .on('error', (error) => reject(error))
  })
}

const transformRow = (row, idMap, idKey, hotelId, now) => {
  const id = idMap[row[idKey]]
  if (!id) return null

  return {
    hotel_id: hotelId,
    id_key: id,
    date: row.currentdate,
    room_sold: row.sumrooms,
    revenue: row.sumroomrevenue,
    cancellation: '0',
    no_of_bookings: row.sumrooms,
    created_at: now
  }
}

const transformRows = (rows, idMap, idKey, hotelId) => {
  const now = dayjs().format('YYYY-MM-DD HH:mm:ss')
  return rows
    .map((row) => transformRow(row, idMap, idKey, hotelId, now))
    .filter((transformedRow) => transformedRow !== null)
}

const writeCSV = (data, filePath) => {
  const csvWriter = createCsvWriter({
    path: filePath,
    header: [
      { id: 'hotel_id', title: 'hotel_id' },
      { id: 'id_key', title: 'id_key' },
      { id: 'date', title: 'date' },
      { id: 'room_sold', title: 'room_sold' },
      { id: 'revenue', title: 'revenue' },
      { id: 'cancellation', title: 'cancellation' },
      { id: 'no_of_bookings', title: 'no_of_bookings' },
      { id: 'created_at', title: 'created_at' }
    ]
  })

  return csvWriter.writeRecords(data)
}

const processCSV = async (filePath, idMap, idKey) => {
  try {
    const rows = await readCSV(filePath)
    const transformedRows = transformRows(rows, idMap, idKey, hotelId)
    const genDate = dayjs().format('YYYY-MM-DD')
    const outputPath = `data/output/${filePath.split('/')[1]}_stat_${genDate}.csv`
    await writeCSV(transformedRows, outputPath)
    console.log('CSV file was written successfully')
  } catch (error) {
    console.error('Error processing CSV file:', error)
  }
}

// Process room types CSV
// processCSV('data/HIEX_RT.csv', roomTypes, 'roomtype')

// Process market segments CSV
processCSV('data/HIEX_Seg Mar 24.csv', marketSegments, 'mktseg')
