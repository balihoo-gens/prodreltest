package com.balihoo.fulfillment.adapters

import java.io.Reader

import com.github.tototoshi.csv.CSVReader

trait CsvAdapterComponent {
  def csvAdapter: CsvAdapter
  trait CsvAdapter {
    def parseReaderAsStream(reader: Reader): Stream[List[String]]
  }
}

trait ScalaCsvAdapterComponent extends CsvAdapterComponent {
  override val csvAdapter = new ScalaCsvAdapter
  class ScalaCsvAdapter extends CsvAdapter {
    override def parseReaderAsStream(reader: Reader) = CSVReader.open(reader).toStream()
  }
}