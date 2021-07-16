package com.huemulsolutions.bigdata.sql_decode

class huemul_sql_symbol(symbol_start: String, symbol_end: String, symbol_isForText: Boolean) extends Serializable {
  def getsymbol_start: String = symbol_start
  def getsymbol_end: String = symbol_end
  def getsymbol_isForText: Boolean = symbol_isForText
}