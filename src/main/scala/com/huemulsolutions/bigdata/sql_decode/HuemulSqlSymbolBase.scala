package com.huemulsolutions.bigdata.sql_decode

class HuemulSqlSymbolBase(symbol: String, symbolLine: Int, posStart: Int, posEnd: Int) extends Serializable {
  def getSymbol: String = symbol
  def getSymbolLine: Int = symbolLine
  def getPosStart: Int = posStart
  def getPosEnd: Int = posEnd
}
