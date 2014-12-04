package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.{TempFileOutputStreamContext, TempFileReaderContext}
import com.balihoo.fulfillment.util.{Splogger, SploggerComponent}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

@RunWith(classOf[JUnitRunner])
class TestScalaCsvAdapter extends Specification with Mockito {

  "csvAdapter" should {
    "allow to write and read from a csv given a valid output stream and reader" in new TestContext with TempFileOutputStreamContext with TempFileReaderContext {
      val writer = csvAdapter.newWriter(tempFileOutputStream)
      writer must beAnInstanceOf[component.CsvWriter]
      writer.writeRow(data.headers)
      writer.writeRows(Seq(data.row1, data.row2))
      val stream = csvAdapter.parseReaderAsStream(tempReader).get
      stream must beAnInstanceOf[Stream[List[String]]]
      stream.size must beEqualTo(3)
      stream.head must beEqualTo(data.headers)
      stream.drop(1).head must beEqualTo(data.row1)
      stream.drop(2).head must beEqualTo(data.row2)
    }
    "return a failure to get a stream from an empty reader" in new TestContext with TempFileOutputStreamContext with TempFileReaderContext {
      csvAdapter.parseReaderAsStream(tempReader) must beAFailedTry
    }
  }

  class TestContext extends Scope  {
    object data {
      val headers = Seq("h1", "h2", "h3")
      val row1 = Seq("a1", "a2", "a3")
      val row2 = Seq("b1", "b2", "b3")
    }
    val component = new AnyRef with ScalaCsvAdapterComponent with SploggerComponent {
      override val splog = mock[Splogger]
    }
    val csvAdapter = component.csvAdapter
  }

}
