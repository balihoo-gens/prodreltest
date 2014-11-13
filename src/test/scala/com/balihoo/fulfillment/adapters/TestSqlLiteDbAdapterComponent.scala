package com.balihoo.fulfillment.adapters

import com.balihoo.fulfillment.util.{SploggerComponent, Splogger}
import org.junit.runner.RunWith
import org.specs2.mock.Mockito
import org.specs2.mutable.Specification
import org.specs2.runner.JUnitRunner
import org.specs2.specification.Scope

@RunWith(classOf[JUnitRunner])
class TestSqlLiteDbAdapterComponent extends Specification with Mockito {
  
  "SqlLiteDatabaseAdapter" should {
    "create a ready to use db file and destroy it" in new TestContext {

      val start = System.currentTimeMillis()
      val db = liteDbAdapter.create("sqllite_test")

      db.file.exists must beTrue
      db.isClosed must beFalse

      db.execute("create table recipients (id integer, name string)")

      val dbBatch = db.batch("insert into recipients (id, name) values (?, ?)")

      dbBatch.param(1, 1)
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

      db.close()
      db.isClosed must beTrue
      db.file.exists must beTrue

      db.destroy()
      db.file.exists must beFalse

      println("SQL lite execution time: " + (System.currentTimeMillis - start) + " ms")

    }
  }

  class TestContext extends Scope {

    val component = new AnyRef with SqlLiteLightweightDatabaseAdapterComponent with SploggerComponent {
      override val splog = mock[Splogger]
    }
    val liteDbAdapter = component.liteDbAdapter
  }

}
