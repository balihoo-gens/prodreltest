package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.TempFileContext
import com.balihoo.fulfillment.util.{SploggerComponent, Splogger}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

import scala.util.Try

@RunWith(classOf[JUnitRunner])
class TestSqlLiteDbAdapterComponent extends Specification with Mockito {

  "LightweightDatabaseAdapter" should {
    "return 0 when count is 0" in new TestContext {
      liteDbAdapter.calculatePageCount(0, 1) must beEqualTo(0)
    }
    "return 1 when count is 1 and pageSize is 1" in new TestContext {
      liteDbAdapter.calculatePageCount(1, 1) must beEqualTo(1)
    }
    "return 3 when count is 5 and pageSize is 2" in new TestContext {
      liteDbAdapter.calculatePageCount(5, 2) must beEqualTo(3)
    }
    "return 2 when count is 4 and pageSize is 2" in new TestContext {
      liteDbAdapter.calculatePageCount(4, 2) must beEqualTo(2)
    }
    "throw exception when pageSize parameter is invalid" in new TestContext {
      Try(liteDbAdapter.calculatePageCount(1, 0)) must beAFailedTry.withThrowable[IllegalArgumentException]
    }
  }

  "SqlLiteDatabaseAdapter" should {
    "return a lightweight database" in new TestContext with TempFileContext {
      val db = liteDbAdapter.create(tempFile)
      db must beAnInstanceOf[component.LightweightDatabase]
      db.close()
    }
  }
  "SqlLiteDatabase" should {
    "execute and selectCount" in new SqlLiteDbContext {
      db.execute(data.createTableSql)
      db.execute(data.insertsSql.head)
      val selectCount = db.selectCount("select count(id) from recipients where name like 'r%'")
      selectCount must beEqualTo(1)
    }
    "batch" in new SqlLiteDbContext {
      db.execute("create table recipients (id integer, name string)")
      val dbBatch = db.batch("insert into recipients (id, name) values (?, ?)")
      dbBatch.param(1, 1.toLong)
      dbBatch.param(2, "roger")
      dbBatch.add()
      dbBatch.param(1, 2)
      dbBatch.param(2, "rafael")
      dbBatch.add()
      dbBatch.param(1, 3)
      dbBatch.param(2, "novak")
      dbBatch.add()
      dbBatch.execute()
      val selectCount = db.selectCount("select count(id) from recipients where name like 'r%'")
      selectCount must beEqualTo(2)
    }
    "fail pagedSelect if statement is missing an order by clause" in new SqlLiteDbContext {
      val result = Try(db.pagedSelect("select * from recipients", data.insertsSql.size))
      result must beAFailedTry.withThrowable[IllegalArgumentException]
    }
    "fail pagedSelect pageSize is invalid" in new SqlLiteDbContext {
      val result = Try(db.pagedSelect("select * from recipients order by id", data.insertsSql.size, 0))
      result must beAFailedTry.withThrowable[IllegalArgumentException]
    }
    "pagedSelect should give paged access to table rows" in new SqlLiteDbContext {
      db.execute(data.createTableSql)
      data.insertsSql.foreach(db.execute)

      val result = Try(db.pagedSelect("select id, name, bday from recipients order by id", data.insertsSql.size, 2))
      result must beSuccessfulTry

      val pages = result.get

      pages.hasNext must beTrue
      val page1 = pages.next
      page1.hasNext must beTrue
      val row1 = page1.next
      row1 must contain(exactly(1, "Rick", "1986-08-19"))
      page1.hasNext must beTrue
      val row2 = page1.next
      row2 must contain(exactly(2, "Sam", "1980-04-01"))
      page1.hasNext must beFalse

      pages.hasNext must beTrue
      val page2 = pages.next
      page2.hasNext must beTrue
      val row3 = page2.next
      row3.size must beEqualTo(3)
      row3 must contain(allOf(3, "Lucy"))
      page2.hasNext must beTrue
      val row4 = page2.next
      row4.size must beEqualTo(3)
      row4 must contain(allOf(4, "Lacy"))
      page2.hasNext must beFalse

      pages.hasNext must beTrue
      val page3 = pages.next
      page3.hasNext must beTrue
      val row5 = page3.next
      row5.size must beEqualTo(3)
      row5 must contain(allOf(5, "Sarah"))
      page3.hasNext must beFalse
    }
  }

  trait SqlLiteDbContext extends TestContext with TempFileContext {
    val db = liteDbAdapter.create(tempFile)
    override def after = {
      db.close()
      super.after
    }
  }

  class TestContext extends Scope {

    object data {
      val createTableSql = "create table recipients (id integer, name varchar, bday date null)"
      val insertsSql = Seq(
        "insert into recipients (id, name, bday) values (1, 'Rick', '1986-08-19')",
        "insert into recipients (id, name, bday) values (2, 'Sam', '1980-04-01')",
        "insert into recipients (id, name) values (3, 'Lucy')",
        "insert into recipients (id, name) values (4, 'Lacy')",
        "insert into recipients (id, name) values (5, 'Sarah')")
    }

    val component = new AnyRef with SqlLiteLightweightDatabaseAdapterComponent with SploggerComponent {
      override val splog = mock[Splogger]
    }
    val liteDbAdapter = component.liteDbAdapter
  }

}
