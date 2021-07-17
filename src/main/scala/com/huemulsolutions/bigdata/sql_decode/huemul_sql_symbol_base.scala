package com.huemulsolutions.bigdata.sql_decode

class huemul_sql_symbol_base(symbol: String, symbol_line: Int, pos_start: Int, pos_end: Int) extends Serializable {
  def getsymbol: String = symbol
  def getsymbol_line: Int = symbol_line
  def getpos_start: Int = pos_start
  def getpos_end: Int = pos_end
}
