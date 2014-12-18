package com.balihoo.fulfillment.workers.datastore

import org.junit.runner._
import org.specs2.mock.Mockito
import org.specs2.mutable._
import org.specs2.runner._
import org.specs2.specification.Scope

@RunWith(classOf[JUnitRunner])
class TestDatabaseColumnDefinition extends Specification with Mockito {

  "ColumnDefinition" should {
    "detect text types" in new DbTestContext {
      DatabaseColumnDefinition("aname", "nvarchar", "asource").dataType must beEqualTo(DatabaseTypes.Text)
      DatabaseColumnDefinition("aname", "char(3)", "asource").dataType must beEqualTo(DatabaseTypes.Text)
      DatabaseColumnDefinition("aname", "varchar(100)", "asource").dataType must beEqualTo(DatabaseTypes.Text)
      DatabaseColumnDefinition("aname", "text", "asource").dataType must beEqualTo(DatabaseTypes.Text)
    }
    "detect integer types" in new DbTestContext {
      DatabaseColumnDefinition("aname", "int", "asource").dataType must beEqualTo(DatabaseTypes.Integer)
      DatabaseColumnDefinition("aname", "integer", "asource").dataType must beEqualTo(DatabaseTypes.Integer)
      DatabaseColumnDefinition("aname", "bigint", "asource").dataType must beEqualTo(DatabaseTypes.Integer)
      DatabaseColumnDefinition("aname", "smallint", "asource").dataType must beEqualTo(DatabaseTypes.Integer)
    }
    "detect real types" in new DbTestContext {
      DatabaseColumnDefinition("aname", "real", "asource").dataType must beEqualTo(DatabaseTypes.Real)
      DatabaseColumnDefinition("aname", "float", "asource").dataType must beEqualTo(DatabaseTypes.Real)
      DatabaseColumnDefinition("aname", "double", "asource").dataType must beEqualTo(DatabaseTypes.Real)
    }
    "detect date types" in new DbTestContext {
      DatabaseColumnDefinition("aname", "date", "asource").dataType must beEqualTo(DatabaseTypes.Date)
    }
    "detect timestamp types" in new DbTestContext {
      DatabaseColumnDefinition("aname", "datetime", "asource").dataType must beEqualTo(DatabaseTypes.Timestamp)
      DatabaseColumnDefinition("aname", "timestamp", "asource").dataType must beEqualTo(DatabaseTypes.Timestamp)
    }
  }

  class DbTestContext extends Scope {
    object data {
      val col1 = DatabaseColumnDefinition("someint", "integer", "SOMEINT", Some("primary key"))
      val col2 = DatabaseColumnDefinition("someint2", "integer", "SOMEINT2", Some("primary key"))
      val col3 = DatabaseColumnDefinition("somestring", "varchar(50)", "SOMESTRING", Some("unique"))
      val col4 = DatabaseColumnDefinition("somedate", "date", "SOMEDATE")
      val col5 = DatabaseColumnDefinition("somestring2", "char(3)", "SOMESTRING2", Some("an_index_name"))
      val col6 = DatabaseColumnDefinition("somestring3", "char(3)", "SOMESTRING3", Some("an_index_name"))
      val col7 = DatabaseColumnDefinition("somebool", "boolean", "SOMEBOOL")
      val col8 = DatabaseColumnDefinition("sometimestamp", "timestamp", "SOMETIMESTAMP")
      val col9 = DatabaseColumnDefinition("somereal", "double", "SOMEREAL")
      val tableDefinition =
        DatabaseTableDefinition(
          Seq(
            col1,
            col2,
            col3,
            col4,
            col5,
            col6,
            col7,
            col8,
            col9
          )
        )
    }
  }
}
