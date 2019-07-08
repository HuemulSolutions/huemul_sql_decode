package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._
//import com.huemulsolutions.bigdata.huemul_sql_decode

class huemul_sql_decode_result extends Serializable {
  var AliasQuery: String = null
  var AliasDatabase: String = null
  
  var columns: ArrayBuffer[huemul_sql_columns] = new ArrayBuffer[huemul_sql_columns]()
  var columns_where: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
  //var columns_groupby: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
  //var columns_having: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
  
  var tables: ArrayBuffer[huemul_sql_tables] = new ArrayBuffer[huemul_sql_tables]()
  var from_sql: String = "" 
  var where_sql: String = ""
  //var group_by_sql: String = ""
  //var having_sql: String = ""
  var subquery_result: ArrayBuffer[huemul_sql_decode_result] = new ArrayBuffer[huemul_sql_decode_result] ()
  
  var AutoIncSubQuery: Int = 0
  var NumErrors: Int = 0
}