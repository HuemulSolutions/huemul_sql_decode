package com.huemulsolutions.bigdata.sql_decode

class HuemulSqlSymbol(symbolStart: String, symbolEnd: String, symbolIsForText: Boolean) extends Serializable {
  def getSymbolStart: String = symbolStart
  def getSymbolEnd: String = symbolEnd
  def getSymbolIsForText: Boolean = symbolIsForText
}