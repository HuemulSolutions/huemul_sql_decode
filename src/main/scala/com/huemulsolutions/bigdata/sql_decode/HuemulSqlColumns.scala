package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._

class HuemulSqlColumns extends Serializable {
  var columnName: String = _
  var columnSql: String = _
  var sqlPosStart: Int = 0
  var sqlPosEnd: Int = 0
  var columnOrigin: ArrayBuffer[HuemulSqlColumnsOrigin] = new ArrayBuffer[HuemulSqlColumnsOrigin]()
}