package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._

class huemul_sql_columns extends Serializable {
  var column_name: String = _
  var column_sql: String = _
  var sql_pos_start: Int = 0
  var sql_pos_end: Int = 0
  var column_origin: ArrayBuffer[huemul_sql_columns_origin] = new ArrayBuffer[huemul_sql_columns_origin]()
}