package com.balihoo.fulfillment.adapters

import java.io.{OutputStream, Reader}

import com.github.tototoshi.csv._

/**
 * Component with a CSV adapter.
 */
trait CsvAdapterComponent {

  /**
   * @return CSV adapter instance.
   */
  def csvAdapter: CsvAdapter

  /**
   * Allows interactions with a CSV file.
   */
  trait CsvAdapter {

    /**
     * @return a stream of rows from a `Reader`.
     */
    def parseReaderAsStream(reader: Reader): Stream[List[String]]

    /**
     * @return a CSV writer.
     */
    def newWriter(os: OutputStream): CsvWriter
  }

  /**
   * Allows writing to a CSV file.
   */
  trait CsvWriter {

    /**
     * Write a list of fields.
     */
    def writeRow(fields: Seq[Any])


    /**
     * Write a list of rows.
     */
    def writeRows(fields: Seq[Seq[Any]])
  }
}

/**
 * ScalaCSV based component.
 */
trait ScalaCsvAdapterComponent extends CsvAdapterComponent {

  override val csvAdapter = new ScalaCsvAdapter

  class ScalaCsvAdapter extends CsvAdapter {

    object csvFormat extends DefaultCSVFormat {
      override val quoting: Quoting = QUOTE_ALL
    }

    override def parseReaderAsStream(reader: Reader): Stream[List[String]] = CSVReader.open(reader)(csvFormat).toStream()

    override def newWriter(os: OutputStream): CsvWriter = new ScalaCsvWriter(CSVWriter.open(os)(csvFormat))

  }

  class ScalaCsvWriter(csvWriter: com.github.tototoshi.csv.CSVWriter) extends CsvWriter {

    override def writeRow(fields: Seq[Any]) = csvWriter.writeRow(fields)

    override def writeRows(allRows: Seq[Seq[Any]]) = csvWriter.writeAll(allRows)

  }

}