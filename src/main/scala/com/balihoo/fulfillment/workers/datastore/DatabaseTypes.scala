package com.balihoo.fulfillment.workers.datastore

/**
  * Supported database types enumeration.
  */
object DatabaseTypes {
   sealed abstract class DataType(val aliases: String*) {
     val regex = aliases
       .map(_.replaceAll("\\s", "\\\\s"))
       .mkString("(", "|", ")\\s*([(](\\d+)[)]){0,1}")
       .r
   }
   case object Text extends DataType("character", "varchar", "varying character", "char", "nchar", "native character", "nvarchar", "text", "clob")
   case object Integer extends DataType("int", "integer", "tinyint", "smallint", "mediumint", "bigint", "unsigned big int", "int2", "int8")
   case object Real extends DataType("real", "double", "double precision", "float")
   case object Date extends DataType("date")
   case object Boolean extends DataType("boolean")
   case object Timestamp extends DataType("datetime", "timestamp")

   val AllSupportedAliases = Boolean.aliases ++ Date.aliases ++ Integer.aliases ++ Real.aliases ++ Text.aliases ++ Timestamp.aliases
}