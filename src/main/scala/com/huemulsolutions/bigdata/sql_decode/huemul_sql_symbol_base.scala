package com.huemulsolutions.bigdata.sql_decode

class huemul_sql_symbol_base(symbol: String, symbol_line: Int, pos_start: Int, pos_end: Int) extends Serializable {
  def getsymbol: String = {return symbol}
  def getsymbol_line: Int = {return symbol_line}
  def getpos_start: Int = {return pos_start} 
  def getpos_end: Int = {return pos_end} 
}
