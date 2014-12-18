package com.balihoo.fulfillment.workers.datastore

import play.api.libs.json.{JsArray, JsObject, JsString, Json}

import scala.collection.immutable.TreeSet

/**
 * Error thrown when an invalid column specified in query.
 */
case class DatabaseInvalidQueryColumnException(columns: Set[String])
  extends RuntimeException("Invalid columns names: " + columns.mkString(", "))

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
case class DatabaseQueryDefinition(select: JsObject, tableName: Option[String] = Some("recipients")) {

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
   * `true` if at least one column is defined.
   */
  val hasColumns = columns.nonEmpty

  /**
   * Sorted select column definitions.
   */
  val sortedColumnDefs = select.fields.sortBy(_._1)

  /**
   * Check if all columns in a set are valid.
   * @throws DatabaseInvalidQueryColumnException if some fields invalids.
   */
  def checkColumns(dbColumns: Set[String]) = {
    val invalidFields = columns.diff(dbColumns)
    if (invalidFields.nonEmpty) throw DatabaseInvalidQueryColumnException(invalidFields)
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
   * Determine if this query has criterions.
   * It is possible to define a query with columns without any criterions.
   */
  val hasCriterions = column2criterions.map(_._2).filter(_.nonEmpty).nonEmpty

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
  val whereExpression =
    if (hasCriterions) "where " + criterionExpressions.mkString(" and ") else ""

  /**
   * Order by expression, if at least one column is defined.
   */
  val orderByExpression =
    if (hasColumns) s"""order by "${columns.head}"""" else ""

  /**
   * From expression with table name.
   */
  val fromExpression = s"""from "$getTableName""""

  /**
   * A sql select statement to get a projected query count.
   */
  val selectCountSql = {
    val b = new StringBuilder()
    b ++= "select count(*) "
    b ++= fromExpression
    if (hasCriterions) b ++= " " ++= whereExpression
    b.toString()
  }

  /**
   * A sql select expression based on defined columns.
   */
  val selectExpression = {
    if (columns.isEmpty)
      "select *"
    else
      s"select $columnsExpression"
  }

  /**
   * A complete sql select statement based on current query spec.
   */
  val selectSql = {
    val b = new StringBuilder()
    b ++= selectExpression
    b ++= " " ++= fromExpression
    if (hasCriterions) b ++= " " ++= whereExpression
    if (columns.nonEmpty) b ++= " " ++= orderByExpression
    b.toString()
  }

  /**
   * @param column the column on which we group the count
   * @param values restrict the column values
   * @return a select expression which yield a string column and a count of results for that column.
   *         If restricting values are specified, then it adds that in the where expression.
   */
  def selectCountOnColumn(column: String, values: Set[String] = Set.empty) = {
    require(column.trim.nonEmpty)
    val b = new StringBuilder()
    b ++= "select "
    b ++= "\"" ++= column ++= "\""
    b ++= ", count(*) "
    b ++= fromExpression
    if (hasCriterions) b ++= " " ++= whereExpression
    if (values.nonEmpty) {
      val newCriterion = values.map("'" + _ + "'").mkString("(", ",", ")")
      if (hasCriterions) b ++= " and \"" ++= column ++= "\" in " ++= newCriterion
      else b ++= " where \"" ++= column ++= "\" in " ++= newCriterion
    }
    b ++= " group by \"" ++= column ++= "\""
    b ++= " order by \"" ++= column ++= "\""
    b.toString()
  }
}

object DatabaseQueryDefinition {
  implicit val jsonFormat = Json.format[DatabaseQueryDefinition]
}