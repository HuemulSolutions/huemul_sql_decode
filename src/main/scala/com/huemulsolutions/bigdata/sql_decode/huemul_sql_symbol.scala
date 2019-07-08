package com.huemulsolutions.bigdata.sql_decode

class huemul_sql_symbol(symbol_start: String, symbol_end: String, symbol_isForText: Boolean) extends Serializable {
  def getsymbol_start: String = {return symbol_start}
  def getsymbol_end: String = {return symbol_end}
  def getsymbol_isForText: Boolean = {return symbol_isForText}
}