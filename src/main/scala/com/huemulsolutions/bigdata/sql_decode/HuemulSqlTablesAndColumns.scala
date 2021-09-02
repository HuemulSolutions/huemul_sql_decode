package com.huemulsolutions.bigdata.sql_decode

/**
 * Used for specify tables and columns info (for example, from hive metadata)
 */
class HuemulSqlTablesAndColumns extends Serializable {
  var databaseName: String = _
  var tableName: String = _
  var columnName: String = _

  def setData(databaseName: String, tableName: String, columnName: String): HuemulSqlTablesAndColumns = {
    this.tableName = tableName
    this.databaseName = databaseName
    this.columnName = columnName
    
    this
  }
}