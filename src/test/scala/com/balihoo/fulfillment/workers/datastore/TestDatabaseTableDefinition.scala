package com.balihoo.fulfillment.workers.datastore

import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class TestDatabaseTableDefinition extends Specification with Mockito {

  "TableDefinition" should {
    "populate source to name map" in new DbTestContext {
      val result = data.tableDefinition.source2name
      result must beAnInstanceOf[Map[String, String]]
      result.size must beEqualTo(data.tableDefinition.columns.size)
      result.getOrElse("SOMEINT".toLowerCase, "") must beEqualTo("someint")
      result.getOrElse("SOMEINT2".toLowerCase, "") must beEqualTo("someint2")
      result.getOrElse("SOMESTRING".toLowerCase, "") must beEqualTo("somestring")
      result.getOrElse("SOMEDATE".toLowerCase, "") must beEqualTo("somedate")
      result.getOrElse("SOMESTRING2".toLowerCase, "") must beEqualTo("somestring2")
      result.getOrElse("SOMESTRING3".toLowerCase, "") must beEqualTo("somestring3")
      result.getOrElse("SOMEBOOL".toLowerCase, "") must beEqualTo("somebool")
      result.getOrElse("SOMETIMESTAMP".toLowerCase, "") must beEqualTo("sometimestamp")
      result.getOrElse("SOMEREAL".toLowerCase, "") must beEqualTo("somereal")
    }
    "populate names to db types map" in new DbTestContext {
      val result = data.tableDefinition.name2type
      result must beAnInstanceOf[Map[String, String]]
      result.size must beEqualTo(data.tableDefinition.columns.size)
      result.getOrElse("someint", "") must beEqualTo(DatabaseTypes.Integer)
      result.getOrElse("someint2", "") must beEqualTo(DatabaseTypes.Integer)
      result.getOrElse("somestring", "") must beEqualTo(DatabaseTypes.Text)
      result.getOrElse("somedate", "") must beEqualTo(DatabaseTypes.Date)
      result.getOrElse("somestring2", "") must beEqualTo(DatabaseTypes.Text)
      result.getOrElse("somestring3", "") must beEqualTo(DatabaseTypes.Text)
      result.getOrElse("somebool", "") must beEqualTo(DatabaseTypes.Boolean)
      result.getOrElse("sometimestamp", "") must beEqualTo(DatabaseTypes.Timestamp)
      result.getOrElse("somereal", "") must beEqualTo(DatabaseTypes.Real)
    }
    "generate appropriate table creation sql statement" in new DbTestContext {
      val result = data.tableDefinition.tableCreateSql
      result must beAnInstanceOf[String]
      result must beEqualTo("create table \"recipients\" (" +
        "\"someint\" integer, " +
        "\"someint2\" integer, " +
        "\"somestring\" varchar(50), " +
        "\"somedate\" date, " +
        "\"somestring2\" char(3), " +
        "\"somestring3\" char(3), " +
        "\"somebool\" boolean, " +
        "\"sometimestamp\" timestamp, " +
        "\"somereal\" double, " +
        "primary key (\"someint\", \"someint2\"))")
    }
    "generate appropriate unique indexes creation sql statements" in new DbTestContext {
      val uniques = data.tableDefinition.uniqueIndexCreateSql
      uniques must beAnInstanceOf[Seq[String]]
      uniques must beEqualTo(Seq("create unique index \"somestring_unique_idx\" on \"recipients\" (\"somestring\")"))
    }
    "generate appropriate simple indexes creation sql statements" in new DbTestContext {
      val simples = data.tableDefinition.simpleIndexCreateSql
      simples must beAnInstanceOf[Seq[String]]
      simples must beEqualTo(Seq("create index \"an_index_name\" on \"recipients\" (\"somestring2\", \"somestring3\")"))
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
