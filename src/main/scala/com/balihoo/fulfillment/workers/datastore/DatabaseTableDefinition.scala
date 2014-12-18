package com.balihoo.fulfillment.workers.datastore

/**
  * Simple SQL table definition.
  */
case class DatabaseTableDefinition(columns: Seq[DatabaseColumnDefinition] = Seq.empty, name: Option[String] = Some("recipients")) {

  /** Source to name mapping. */
  val source2name = columns.map(col => col.getSource -> col.getName).toMap

  /** Name to data type mapping. */
  val name2type = columns.map(col => col.getName -> col.dataType).toMap

  /** Index names to indexed column definitions. */
  val indexedColumns = columns.filter(_.isIndexed)

  /** Primary key column definitions */
  val primaryKeyIndexColumns = indexedColumns.filter(_.isPrimaryKey)

  /** Unique indexes. */
  val uniqueIndexColumns = indexedColumns.filter(_.isUniqueKey)

  /** Simple indexes. */
  val simpleIndexColumns = indexedColumns.filter(_.isSimpleIndex)

  /** Primary key creation sql statement. */
  val primaryKeyCreateSql = primaryKeyIndexColumns.map("\"" + _.getName + "\"").mkString("primary key (", ", ", ")")

  /** Table name to use. */
  val getName = name.getOrElse("recipients")

  /** Return a data definition SQL statement from this table definition. */
  val tableCreateSql = {
   val columnsCreateSql = columns.map(_.columnCreateSql)
   val allDefinitions = if (primaryKeyIndexColumns.isEmpty) columnsCreateSql else columnsCreateSql :+ primaryKeyCreateSql
   allDefinitions.mkString(s"""create table "$getName" (""", ", ", ")")
  }

  /** Return a list of create SQL statement for unique indexes defined in this table definition. */
  val uniqueIndexCreateSql = uniqueIndexColumns.map({ column =>
   s"""create unique index "${column.getName}_unique_idx" on "$getName" ("${column.getName}")"""
  }).toSeq

  /** Return a list of create SQL statement for simple indexes defined in this table definition. */
  val simpleIndexCreateSql = simpleIndexColumns
   .groupBy(_.getIndex)
   .map({ case (indexName, indexColumnDefinitions) =>
     val indexColumns = indexColumnDefinitions.map("\"" + _.getName + "\"").mkString(", ")
     s"""create index "$indexName" on "$getName" ($indexColumns)"""
   }).toSeq

}
