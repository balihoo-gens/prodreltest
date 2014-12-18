package com.balihoo.fulfillment.workers.datastore

/**
  * Simple SQL column definition.
  */
case class DatabaseColumnDefinition(name: String, `type`: String, source: String, index: Option[String] = None) {

  /** members as lower case */

  val getName = name.toLowerCase
  val getType = `type`.toLowerCase
  val getSource = source.toLowerCase
  def getIndex = index.get.toLowerCase

  /** Simple column creation sql statement. */
  val columnCreateSql = "\"" + getName + "\" " + getType

  /** Column is indexed. */
  val isIndexed = index.isDefined

  /** Column part of primary key. */
  def isPrimaryKey = index.fold(false) {
   case idx if getIndex matches "primary key|pk|primary|primarykey" => true
   case _ => false
  }

  /** Column has unique index. */
  def isUniqueKey = index.fold(false) {
   case idx if getIndex matches "unique" => true
   case _ => false
  }

  /** Index is simple. */
  def isSimpleIndex = isIndexed && !(isUniqueKey || isPrimaryKey)

  /**
  * @return Data type from the raw column type.
  */
  def dataType = getType match {
   case DatabaseTypes.Text.regex(keywords, _, size) => DatabaseTypes.Text
   case DatabaseTypes.Integer.regex(keywords, _, size) => DatabaseTypes.Integer
   case DatabaseTypes.Real.regex(keywords, _, size) => DatabaseTypes.Real
   case DatabaseTypes.Date.regex(keywords, _, size) => DatabaseTypes.Date
   case DatabaseTypes.Boolean.regex(keywords, _, size) => DatabaseTypes.Boolean
   case DatabaseTypes.Timestamp.regex(keywords, _, size) => DatabaseTypes.Timestamp
   case _ => throw new RuntimeException(s"unsupported db data type [${`type`}]")
  }
}
