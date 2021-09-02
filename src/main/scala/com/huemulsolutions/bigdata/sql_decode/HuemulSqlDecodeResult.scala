package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._
//import com.huemulsolutions.bigdata.huemul_sql_decode

class HuemulSqlDecodeResult extends Serializable {
  var aliasQuery: String = _
  var aliasDatabase: String = _
  
  var columns: ArrayBuffer[HuemulSqlColumns] = new ArrayBuffer[HuemulSqlColumns]()
  var columnsWhere: ArrayBuffer[HuemulSqlColumnsOrigin] = new ArrayBuffer[HuemulSqlColumnsOrigin]()
  //var columns_groupby: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
  //var columns_having: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
  
  var tables: ArrayBuffer[HuemulSqlTables] = new ArrayBuffer[HuemulSqlTables]()
  var fromSql: String = ""
  var whereSql: String = ""
  //var group_by_sql: String = ""
  //var having_sql: String = ""
  var subQueryResult: ArrayBuffer[HuemulSqlDecodeResult] = new ArrayBuffer[HuemulSqlDecodeResult] ()
  
  var autoIncSubQuery: Int = 0
  var numErrors: Int = 0
}