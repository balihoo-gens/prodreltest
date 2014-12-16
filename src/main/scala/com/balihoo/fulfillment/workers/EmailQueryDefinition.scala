package com.balihoo.fulfillment.workers

import play.api.libs.json.{JsArray, JsObject, JsString}

import scala.collection.immutable.TreeSet

/**
 * Error thrown when an invalid column specified in query.
 */
case class EmailInvalidQueryColumnException(columns: Set[String])
  extends RuntimeException("Invalid columns names: " + columns.mkString(", "))

/**
 * Error thrown when no columns specified in select.
 */
class EmailNoQueryColumnException
  extends ActivitySpecificationException("No columns specified in select attribute")

/**
 * SQL query definition.
 *
 * The `select` field contains a set of sql criterion(s) per field.
 * Format is 1 json object per field name and this object contains
 * either a single string or an array of strings. Those strings are
 * sql where clause criterions to be combined to produce a sql query.
 * These criterions are combined with sql logical disjunctions (`or`)
 * at the field level and combined again with a sql logical
 * conjunction (`and`) at the select level.
 *
 * Utilities helps building the end where clause and select expression.
 */
case class EmailQueryDefinition(select: JsObject, tableName: Option[String] = Some("recipients")) {

  if (select.fields.isEmpty) throw new EmailNoQueryColumnException

  /**
   * Placeholder in json model for field name in clauses.
   */
  val columnNamePlaceholder = "\\$v"

  /**
   * Get table name to use for query.
   */
  val getTableName = tableName.getOrElse("recipients")

  /**
   * List of column names to be returned by select statement.
   */
  val columns = TreeSet(select.fields.map(_._1):_*)

  /**
   * Sorted select column definitions.
   */
  val sortedColumnDefs = select.fields.sortBy(_._1)

  /**
   * Check if all columns in a set are valid.
   * @throws EmailInvalidQueryColumnException if some fields invalids.
   */
  def checkColumns(dbColumns: Set[String]) = {
    val invalidFields = columns.diff(dbColumns)
    if (invalidFields.nonEmpty) throw EmailInvalidQueryColumnException(invalidFields)
  }

  /**
   * Map column name to a sequence of sql criterions.
   */
  val column2criterions = sortedColumnDefs.flatMap { case (name, jsValue) =>
    jsValue match {
      /* Only support non empty string and array of strings for now. */
      case JsArray(elements) if elements.nonEmpty => Some(name -> elements.map(_.as[String]))
      case JsString(value) if value.trim.nonEmpty => Some(name -> Seq(value))
      case _ => None
    }
  }

  /**
   * Map field name to a an expression (combined criterions)
   */
  val criterionExpressions = column2criterions.map { case (name, criterions) =>
    val innerCriterions = criterions.map("(" + _.replaceAll(columnNamePlaceholder, s""""$name"""") + ")")
    criterions.size match {
      case size if size > 1 => innerCriterions.mkString("(", " or ", ")")
      case _ => innerCriterions.mkString("")
    }
  }

  /**
   * SQL select columns expression.
   */
  val columnsExpression = columns.map("\"" + _ + "\"").mkString(", ")

  /**
   * SQL where expression.
   */
  val whereExpression = criterionExpressions.mkString(" and ")

  /**
   * SQL select expression to get projected query count.
   */
  val selectCountSql = s"""select count(*) from "$getTableName" where $whereExpression"""

  /**
   * SQL select expression to get query results.
   */
  val selectSql = s"""select $columnsExpression from "$getTableName" where $whereExpression order by "${columns.head}""""

  /**
   * Check generated SQL is valid.
   */
  def validate() = {
    if (whereExpression.isEmpty) throw new RuntimeException("SQL is empty")
    if (whereExpression.contains(";")) throw new RuntimeException("SQL contains reserved separator ';'")
  }

  /**
   * @param column the column on which we group the count
   * @param values restrict the column values
   * @return a select expression which yield a string column and a count of results for that column.
   *         If restricting values are specified, then it adds that in the where expression.
   */
  def selectCountOnColumn(column: String, values: Set[String] = Set.empty) = {
    require(column.trim.nonEmpty)

    val restrictingExpr = if (values.nonEmpty) {
      val restrictingValues = values.map("'" + _ + "'").mkString("(", ",", ")")
      s""" and "$column" in $restrictingValues"""
    } else ""

    s"""select "$column", count(*) from "$getTableName" where $whereExpression$restrictingExpr group by "$column" order by "$column""""
  }
}
