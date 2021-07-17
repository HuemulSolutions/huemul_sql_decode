package com.huemulsolutions.bigdata.sql_decode

/**
 * Used for specify tables and columns info (for example, from hive metadata)
 */
class huemul_sql_tables_and_columns extends Serializable {
  var database_name: String = _
  var table_name: String = _
  var column_name: String = _
  
  def setData(database_name: String, table_name: String, column_name: String): huemul_sql_tables_and_columns = {
    this.table_name = table_name
    this.database_name = database_name
    this.column_name = column_name
    
    this
  }
}