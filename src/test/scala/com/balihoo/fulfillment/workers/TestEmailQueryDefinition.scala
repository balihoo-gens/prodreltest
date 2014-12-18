package com.balihoo.fulfillment.workers

import org.specs2.mutable.Specification
import play.api.libs.json.{JsNumber, Json}

class TestEmailQueryDefinition extends Specification {
  "getTableName" should {
    "use provided table name if specified" in {
      val select = Json.obj("col1" -> Json.arr(), "col2" -> Json.arr())
      EmailQueryDefinition(select, Some("custom name")).getTableName must beEqualTo("custom name")
    }
  }
  "checkFields" should {
    "throw an exception if one of the specified column is not defined" in {
      val select = Json.obj("col1" -> Json.arr(), "col2" -> Json.arr(), "col5" -> Json.arr(), "col6" -> Json.arr())
      val queryDef = EmailQueryDefinition(select)
      queryDef.checkColumns(Set("col1", "col2", "col3", "col4")) must throwA[EmailInvalidQueryColumnException]("Invalid columns names: col5, col6")
    }
  }
  "hasCriterions" should {
    "return true if criterions defined" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).hasCriterions must beTrue
    }
    "return false if no criterions defined" in {
      val select = Json.obj("col2" -> Json.arr(), "col1" -> Json.arr())
      EmailQueryDefinition(select).hasCriterions must beFalse
    }
  }
  "field2criterions" should {
    "map all fields in alphabetical order to a list of their criterions" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      val field2criterions = EmailQueryDefinition(select).column2criterions
      field2criterions must haveSize(2)
      val (field1, criterions1) = field2criterions(0)
      field1 must beEqualTo("col1")
      criterions1 must containTheSameElementsAs(Seq("$v>0"))
      val (field2, criterions2) = field2criterions(1)
      field2 must beEqualTo("col2")
      criterions2 must containTheSameElementsAs(Seq("$v=0", "$v>5 and $v<10"))
    }
    "ignore column definitions that are not of type string or array of string" in {
      val select = Json.obj("col1" -> JsNumber(5))
      val field2criterions = EmailQueryDefinition(select).column2criterions
      field2criterions must haveSize(0)
    }
  }
  "field2criterionsExpression" should {
    "map all fields to a list of their criterions" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).criterionExpressions must
        containTheSameElementsAs(Seq("""("col1">0)""", """(("col2"=0) or ("col2">5 and "col2"<10))"""))
    }
  }
  "columnsExpression" should {
    "return quoted columns list expression for query" in {
      val select = Json.obj("col2" -> Json.arr(), "col1" -> Json.arr())
      EmailQueryDefinition(select).columnsExpression must beEqualTo(""""col1", "col2"""")
    }
  }
  "whereExpression" should {
    "return complete where expression for query" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).whereExpression must beEqualTo("""where ("col1">0) and (("col2"=0) or ("col2">5 and "col2"<10))""")
    }
    "return empty string if no criterions" in {
      val select = Json.obj("col2" -> Json.arr(), "col1" -> Json.arr())
      EmailQueryDefinition(select).whereExpression must beEqualTo("")
    }
  }
  "orderByExpression" should {
    "return complete order by expression for query (first column in list)" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).orderByExpression must beEqualTo("""order by "col1"""")
    }
    "return empty string if no columns" in {
      EmailQueryDefinition(Json.obj()).orderByExpression must beEqualTo("")
    }
  }
  "selectCountSql" should {
    "return sql statement for counting all rows returned by a query" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).selectCountSql must beEqualTo("""select count(*) from "recipients" where ("col1">0) and (("col2"=0) or ("col2">5 and "col2"<10))""")
    }
  }
  "selectSql" should {
    "return sql query and add an order by clause" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).selectSql must beEqualTo("""select "col1", "col2" from "recipients" where ("col1">0) and (("col2"=0) or ("col2">5 and "col2"<10)) order by "col1"""")
    }
  }
  "selectCountOnColumn" should {
    "return sql query for column and count with restricting values" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col3" -> Json.arr(), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).selectCountOnColumn("col3", Set("a", "b", "c")) must beEqualTo("""select "col3", count(*) from "recipients" where ("col1">0) and (("col2"=0) or ("col2">5 and "col2"<10)) and "col3" in ('a','b','c') group by "col3" order by "col3"""")
    }
    "return sql query for column and count without restricting values" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col3" -> Json.arr(), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).selectCountOnColumn("col3") must beEqualTo("""select "col3", count(*) from "recipients" where ("col1">0) and (("col2"=0) or ("col2">5 and "col2"<10)) group by "col3" order by "col3"""")
    }
    "throw an exception if column argument is missing" in {
      val select = Json.obj("col2" -> Json.arr("$v=0", "$v>5 and $v<10"), "col3" -> Json.arr(), "col1" -> Json.arr("$v>0"))
      EmailQueryDefinition(select).selectCountOnColumn(" ") must throwA[IllegalArgumentException]
    }
  }
}
