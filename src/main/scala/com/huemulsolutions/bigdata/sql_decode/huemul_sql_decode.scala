package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._
import util.control.Breaks._
import scala.util.Try

class huemul_SQL_Decode(Symbols_user: ArrayBuffer[String], AutoIncSubQuery: Int ) extends Serializable {
  Symbols_user.append("AS")
  Symbols_user.append(")")
  Symbols_user.append("(")
  Symbols_user.append("CASE")
  Symbols_user.append("THEN")
  Symbols_user.append("END")
  Symbols_user.append("ELSE")
  Symbols_user.append("FROM")
  Symbols_user.append("WHERE")
  Symbols_user.append("GROUP")
  Symbols_user.append("BY")
  Symbols_user.append("HAVING")
  Symbols_user.append("IS")
  Symbols_user.append("NULL")
  //ADD DataTypes
  Symbols_user.append("INT")
  Symbols_user.append("INTEGER")
  Symbols_user.append("STRING")
  Symbols_user.append("VARCHAR")
  Symbols_user.append("VARCHAR2")
  Symbols_user.append("BOOLEAN")
  Symbols_user.append("LONG")
  Symbols_user.append("REAL")
  Symbols_user.append("FLOAT")
  Symbols_user.append("DATE")
  Symbols_user.append("DATETIME")
  Symbols_user.append("TIMESTAMP")
  Symbols_user.append("TINYINT")
  Symbols_user.append("SMALLINT")
  
  val Symbols_joins: ArrayBuffer[String] = new ArrayBuffer[String] 
  Symbols_joins.append("INNER")
  Symbols_joins.append("LEFT")
  Symbols_joins.append("RIGHT")
  Symbols_joins.append("FULL")
  Symbols_joins.append("JOIN")
  
  val Symbols: ArrayBuffer[String] = new ArrayBuffer[String] 
  Symbols.append("+")
  Symbols.append("-")
  Symbols.append("*")
  Symbols.append("/")
  Symbols.append(",")
  Symbols.append(".")
  Symbols.append("%")
  Symbols.append("=")
  Symbols.append(">")
  Symbols.append("<")
  Symbols.append("!")
  
  val SQL_Functions: ArrayBuffer[String] = new ArrayBuffer[String]
  SQL_Functions.append("ABS")
  SQL_Functions.append("ACOS")
  SQL_Functions.append("ADD_MONTHS")
  SQL_Functions.append("AND")
  SQL_Functions.append("APPROX_COUNT_DISTINCT")
  SQL_Functions.append("APPROX_PERCENTILE")
  SQL_Functions.append("ARRAY")
  SQL_Functions.append("ARRAY_CONTAINS")
  SQL_Functions.append("ASCII")
  SQL_Functions.append("ASIN")
  SQL_Functions.append("ASSERT_TRUE")
  SQL_Functions.append("ATAN")
  SQL_Functions.append("ATAN2")
  SQL_Functions.append("AVG")
  SQL_Functions.append("BASE64")
  SQL_Functions.append("BIGINT")
  SQL_Functions.append("BIN")
  SQL_Functions.append("BINARY")
  SQL_Functions.append("BIT_LENGTH")
  SQL_Functions.append("BOOLEAN")
  SQL_Functions.append("BROUND")
  SQL_Functions.append("CAST")
  SQL_Functions.append("CBRT")
  SQL_Functions.append("CEIL")
  SQL_Functions.append("CEILING")
  SQL_Functions.append("CHAR")
  SQL_Functions.append("CHAR_LENGTH")
  SQL_Functions.append("CHARACTER_LENGTH")
  SQL_Functions.append("CHR")
  SQL_Functions.append("COALESCE")
  SQL_Functions.append("COLLECT_LIST")
  SQL_Functions.append("COLLECT_SET")
  SQL_Functions.append("CONCAT")
  SQL_Functions.append("CONCAT_WS")
  SQL_Functions.append("CONV")
  SQL_Functions.append("CORR")
  SQL_Functions.append("COS")
  SQL_Functions.append("COSH")
  SQL_Functions.append("COT")
  SQL_Functions.append("COUNT")
  SQL_Functions.append("COUNT_MIN_SKETCH")
  SQL_Functions.append("COVAR_POP")
  SQL_Functions.append("COVAR_SAMP")
  SQL_Functions.append("CRC32")
  SQL_Functions.append("CUBE")
  SQL_Functions.append("CUME_DIST")
  SQL_Functions.append("CURRENT_DATABASE")
  SQL_Functions.append("CURRENT_DATE")
  SQL_Functions.append("CURRENT_TIMESTAMP")
  SQL_Functions.append("DATE")
  SQL_Functions.append("DATE_ADD")
  SQL_Functions.append("DATE_FORMAT")
  SQL_Functions.append("DATE_SUB")
  SQL_Functions.append("DATE_TRUNC")
  SQL_Functions.append("DATEDIFF")
  SQL_Functions.append("DAY")
  SQL_Functions.append("DAYOFMONTH")
  SQL_Functions.append("DAYOFWEEK")
  SQL_Functions.append("DAYOFYEAR")
  SQL_Functions.append("DECIMAL")
  SQL_Functions.append("DECODE")
  SQL_Functions.append("DEGREES")
  SQL_Functions.append("DENSE_RANK")
  SQL_Functions.append("DOUBLE")
  SQL_Functions.append("E")
  SQL_Functions.append("ELT")
  SQL_Functions.append("ENCODE")
  SQL_Functions.append("EXP")
  SQL_Functions.append("EXPLODE")
  SQL_Functions.append("EXPLODE_OUTER")
  SQL_Functions.append("EXPM1")
  SQL_Functions.append("FACTORIAL")
  SQL_Functions.append("FIND_IN_SET")
  SQL_Functions.append("FIRST")
  SQL_Functions.append("FIRST_VALUE")
  SQL_Functions.append("FLOAT")
  SQL_Functions.append("FLOOR")
  SQL_Functions.append("FORMAT_NUMBER")
  SQL_Functions.append("FORMAT_STRING")
  SQL_Functions.append("FROM_JSON")
  SQL_Functions.append("FROM_UNIXTIME")
  SQL_Functions.append("FROM_UTC_TIMESTAMP")
  SQL_Functions.append("GET_JSON_OBJECT")
  SQL_Functions.append("GREATEST")
  SQL_Functions.append("GROUPING")
  SQL_Functions.append("GROUPING_ID")
  SQL_Functions.append("HASH")
  SQL_Functions.append("HEX")
  SQL_Functions.append("HOUR")
  SQL_Functions.append("HYPOT")
  SQL_Functions.append("IF")
  SQL_Functions.append("IFNULL")
  SQL_Functions.append("IN")
  SQL_Functions.append("INITCAP")
  SQL_Functions.append("INLINE")
  SQL_Functions.append("INLINE_OUTER")
  SQL_Functions.append("INPUT_FILE_BLOCK_LENGTH")
  SQL_Functions.append("INPUT_FILE_BLOCK_START")
  SQL_Functions.append("INPUT_FILE_NAME")
  SQL_Functions.append("INSTR")
  SQL_Functions.append("INT")
  SQL_Functions.append("ISNAN")
  SQL_Functions.append("ISNOTNULL")
  SQL_Functions.append("ISNULL")
  SQL_Functions.append("JAVA_METHOD")
  SQL_Functions.append("JSON_TUPLE")
  SQL_Functions.append("KURTOSIS")
  SQL_Functions.append("LAG")
  SQL_Functions.append("LAST")
  SQL_Functions.append("LAST_DAY")
  SQL_Functions.append("LAST_VALUE")
  SQL_Functions.append("LCASE")
  SQL_Functions.append("LEAD")
  SQL_Functions.append("LEAST")
  SQL_Functions.append("LEFT")
  SQL_Functions.append("LENGTH")
  SQL_Functions.append("LEVENSHTEIN")
  SQL_Functions.append("LIKE")
  SQL_Functions.append("LN")
  SQL_Functions.append("LOCATE")
  SQL_Functions.append("LOG")
  SQL_Functions.append("LOG10")
  SQL_Functions.append("LOG1P")
  SQL_Functions.append("LOG2")
  SQL_Functions.append("LOWER")
  SQL_Functions.append("LPAD")
  SQL_Functions.append("LTRIM")
  SQL_Functions.append("MAP")
  SQL_Functions.append("MAP_KEYS")
  SQL_Functions.append("MAP_VALUES")
  SQL_Functions.append("MAX")
  SQL_Functions.append("MD5")
  SQL_Functions.append("MEAN")
  SQL_Functions.append("MIN")
  SQL_Functions.append("MINUTE")
  SQL_Functions.append("MOD")
  SQL_Functions.append("MONOTONICALLY_INCREASING_ID")
  SQL_Functions.append("MONTH")
  SQL_Functions.append("MONTHS_BETWEEN")
  SQL_Functions.append("NAMED_STRUCT")
  SQL_Functions.append("NANVL")
  SQL_Functions.append("NEGATIVE")
  SQL_Functions.append("NEXT_DAY")
  SQL_Functions.append("NOT")
  SQL_Functions.append("NOW")
  SQL_Functions.append("NTILE")
  SQL_Functions.append("NULLIF")
  SQL_Functions.append("NVL")
  SQL_Functions.append("NVL2")
  SQL_Functions.append("OCTET_LENGTH")
  SQL_Functions.append("OR")
  SQL_Functions.append("PARSE_URL")
  SQL_Functions.append("PERCENT_RANK")
  SQL_Functions.append("PERCENTILE")
  SQL_Functions.append("PERCENTILE_APPROX")
  SQL_Functions.append("PI")
  SQL_Functions.append("PMOD")
  SQL_Functions.append("POSEXPLODE")
  SQL_Functions.append("POSEXPLODE_OUTER")
  SQL_Functions.append("POSITION")
  SQL_Functions.append("POSITIVE")
  SQL_Functions.append("POW")
  SQL_Functions.append("POWER")
  SQL_Functions.append("PRINTF")
  SQL_Functions.append("QUARTER")
  SQL_Functions.append("RADIANS")
  SQL_Functions.append("RAND")
  SQL_Functions.append("RANDN")
  SQL_Functions.append("RANK")
  SQL_Functions.append("REFLECT")
  SQL_Functions.append("REGEXP_EXTRACT")
  SQL_Functions.append("REGEXP_REPLACE")
  SQL_Functions.append("REPEAT")
  SQL_Functions.append("REPLACE")
  SQL_Functions.append("REVERSE")
  SQL_Functions.append("RIGHT")
  SQL_Functions.append("RINT")
  SQL_Functions.append("RLIKE")
  SQL_Functions.append("ROLLUP")
  SQL_Functions.append("ROUND")
  SQL_Functions.append("ROW_NUMBER")
  SQL_Functions.append("RPAD")
  SQL_Functions.append("RTRIM")
  SQL_Functions.append("SECOND")
  SQL_Functions.append("SENTENCES")
  SQL_Functions.append("SHA")
  SQL_Functions.append("SHA1")
  SQL_Functions.append("SHA2")
  SQL_Functions.append("SHIFTLEFT")
  SQL_Functions.append("SHIFTRIGHT")
  SQL_Functions.append("SHIFTRIGHTUNSIGNED")
  SQL_Functions.append("SIGN")
  SQL_Functions.append("SIGNUM")
  SQL_Functions.append("SIN")
  SQL_Functions.append("SINH")
  SQL_Functions.append("SIZE")
  SQL_Functions.append("SKEWNESS")
  SQL_Functions.append("SMALLINT")
  SQL_Functions.append("SORT_ARRAY")
  SQL_Functions.append("SOUNDEX")
  SQL_Functions.append("SPACE")
  SQL_Functions.append("SPARK_PARTITION_ID")
  SQL_Functions.append("SPLIT")
  SQL_Functions.append("SQRT")
  SQL_Functions.append("STACK")
  SQL_Functions.append("STD")
  SQL_Functions.append("STDDEV")
  SQL_Functions.append("STDDEV_POP")
  SQL_Functions.append("STDDEV_SAMP")
  SQL_Functions.append("STR_TO_MAP")
  SQL_Functions.append("STRING")
  SQL_Functions.append("STRUCT")
  SQL_Functions.append("SUBSTR")
  SQL_Functions.append("SUBSTRING")
  SQL_Functions.append("SUBSTRING_INDEX")
  SQL_Functions.append("SUM")
  SQL_Functions.append("TAN")
  SQL_Functions.append("TANH")
  SQL_Functions.append("TIMESTAMP")
  SQL_Functions.append("TINYINT")
  SQL_Functions.append("TO_DATE")
  SQL_Functions.append("TO_JSON")
  SQL_Functions.append("TO_TIMESTAMP")
  SQL_Functions.append("TO_UNIX_TIMESTAMP")
  SQL_Functions.append("TO_UTC_TIMESTAMP")
  SQL_Functions.append("TRANSLATE")
  SQL_Functions.append("TRIM")
  SQL_Functions.append("TRUNC")
  SQL_Functions.append("UCASE")
  SQL_Functions.append("UNBASE64")
  SQL_Functions.append("UNHEX")
  SQL_Functions.append("UNIX_TIMESTAMP")
  SQL_Functions.append("UPPER")
  SQL_Functions.append("UUID")
  SQL_Functions.append("VAR_POP")
  SQL_Functions.append("VAR_SAMP")
  SQL_Functions.append("VARIANCE")
  SQL_Functions.append("WEEKOFYEAR")
  SQL_Functions.append("WHEN")
  SQL_Functions.append("WINDOW")
  SQL_Functions.append("XPATH")
  SQL_Functions.append("XPATH_BOOLEAN")
  SQL_Functions.append("XPATH_DOUBLE")
  SQL_Functions.append("XPATH_FLOAT")
  SQL_Functions.append("XPATH_INT")
  SQL_Functions.append("XPATH_LONG")
  SQL_Functions.append("XPATH_NUMBER")
  SQL_Functions.append("XPATH_SHORT")
  SQL_Functions.append("XPATH_STRING")
  SQL_Functions.append("YEAR")
  
  val Symbols_keys: ArrayBuffer[huemul_sql_symbol] = new ArrayBuffer[huemul_sql_symbol] 
  Symbols_keys.append(new huemul_sql_symbol("(",")",false))
  Symbols_keys.append(new huemul_sql_symbol("'","'",true))
  Symbols_keys.append(new huemul_sql_symbol(""""""",""""""",true))
  
  val Symbols_drops: ArrayBuffer[String] = new ArrayBuffer[String] 
  Symbols_drops.append("\n")
  
  var line: Int = 0
  private var numTempSubQuery: Int = AutoIncSubQuery
  def getNumTempSubQuery(): Int = {return numTempSubQuery}
  
  /**
   * split a string in characters, search for "\n" and others for split in two o more words
   */
  private def splitKeys(text: String, pos_real_start: Int, pos_real_end: Int): ArrayBuffer[huemul_sql_symbol_base] = {
    val Result: ArrayBuffer[huemul_sql_symbol_base] = new ArrayBuffer[huemul_sql_symbol_base] ()
    var rest_of_text: String = text
    var position_word: Integer = 0
    var cycle: Int = 0
    var text_found: Boolean = false
    var text_found_save: Boolean = true
    var add_line: Boolean = false
    val enter: String = "\n"
    
    var pos_start: Int = pos_real_start
    var pos_end: Int = pos_real_end
    while (position_word < rest_of_text.length()) {
      text_found_save = true
      add_line = false
      text_found = false
      
      if (Symbols.filter { x => x(0) == rest_of_text(position_word)  }.length > 0) 
        text_found = true
      else if (Symbols_keys.filter { x => x.getsymbol_start(0) == rest_of_text(position_word) || x.getsymbol_end(0) == rest_of_text(position_word)  }.length > 0)
        text_found = true
      else if (rest_of_text(position_word) == enter(0)) {
        text_found = true
        text_found_save = false
        add_line = true
      }       
      
      //split text if found special characters
      if (text_found) {
        val text_before = rest_of_text.substring(0,position_word)
        val text_char = rest_of_text.substring(position_word,position_word+1)
        rest_of_text = rest_of_text.substring(position_word+1,rest_of_text.length())
        //Only insert text_before if pos > 0, example  [*campo5] only insert [*] and rest of text [campo5] is for next iteration 
        if (position_word > 0){
          Result.append(new huemul_sql_symbol_base(text_before, line, pos_start, pos_start+position_word))
          pos_start = pos_start+position_word
        }
          
        if (text_found_save) {
          Result.append(new huemul_sql_symbol_base(text_char, line, pos_start, pos_start+1))
          pos_start = pos_start+1
        }
        
        position_word = 0
      } else 
        position_word += 1   
      
      if (add_line) {
        line += 1
        pos_start += 1
      }     
    }
  
    if (rest_of_text.length() > 0)
      Result.append(new huemul_sql_symbol_base(rest_of_text, line, pos_start,pos_end ))
    
    return Result
  }
  
  /**
   * split a text in words, and return only valid words (without spaces and line return)
   */
  private def splitWords(text: String): ArrayBuffer[huemul_sql_symbol_base] = {
    val Result: ArrayBuffer[huemul_sql_symbol_base] = new ArrayBuffer[huemul_sql_symbol_base] ()
    val enter: String = "\n"
    var rest_of_text: String = text
    //println(rest_of_text)
    //Get first word
    var position_end: Integer = 0
    var word: String = null
    var cycle: Int = 0
    line = 1
    var position_real_start: Int = 0
    var position_real_end: Int = 0
    while (position_end >= 0 && cycle <= 20000) {
      position_end = rest_of_text.indexOf(" ")
      position_real_end = position_real_start + position_end
      if (position_end >= 0) {
        word = rest_of_text.substring(0,position_end)
        rest_of_text = rest_of_text.substring(position_end+1, rest_of_text.length())
      } else {
        word = rest_of_text
        position_real_end += rest_of_text.length()+1
        rest_of_text = null
        
      }
            
      if (word != null && word.trim() != "") {
        //split word, searching new characters
        val NewResult = splitKeys(word, position_real_start, position_real_end)
        Result.appendAll(NewResult)                 
      } else if (word == enter)
        line += 1
      
      //println(s"line: $line pos: [$position_end], word: [$word], word len: [${word.length()}]  real ini: $position_real_start, real end: $position_real_end")
      
      position_real_start = position_real_end + 1
      
      cycle += 1
    }
    
    
    return Result
  }
  
  /**
   * remove comments (like --), 
   */
  def Analyze_SQL_RemoveComments(p_words: ArrayBuffer[huemul_sql_symbol_base]): ArrayBuffer[huemul_sql_symbol_base] = {
    var local_words = p_words
    var actual_line = -1
    var position = 0
    var remove_start_pos: Int = -1
    while (position < local_words.length-1) {
      //doesn't have commented lines
      if (actual_line == -1) {
        if (local_words(position).getsymbol == "-" && local_words(position+1).getsymbol == "-") {
          actual_line = local_words(position).getsymbol_line
          remove_start_pos = position
        }
      } else {
        if (actual_line != local_words(position).getsymbol_line) {
          //Remove lines
          local_words.remove(remove_start_pos, position - remove_start_pos )

          //set variables to re-start
          actual_line = -1
          position = remove_start_pos - 1
        }
      }
      
      position += 1
    }
    
    return local_words
  }
  
  /**
   * type for return result of get_select_on_select method
   */
  private class class_get_select_on_select {
    var position: Int = 0
    var select_on_select: huemul_sql_decode_result = null
  }
  
  /**
   * find end of subquerys
   */
  private def get_select_on_select(position: Int, position_max: Int, SQL: String, localWords: ArrayBuffer[huemul_sql_symbol_base], TablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns]): class_get_select_on_select =  {
    //SELECT INSIDE SELECT, GET COLUMN AND TABLES
    val res = new class_get_select_on_select()
      
    var last_pos_close: Int = position + 1
    var NumParen: Int = 1
    var NumText_01: Int = 0
    var NumText_02: Int = 0
    
    //search last position to find close ")"
    while (last_pos_close < position_max && NumParen > 0 ) {
      last_pos_close  += 1
      val word_close = localWords(last_pos_close).getsymbol.toUpperCase()
      
      if (word_close == "'") {
        if (NumText_01 == 0)
          NumText_01 = 1
        else 
          NumText_01 = 0
      }
      else if (word_close == """"""") {
        if (NumText_02 == 0)
          NumText_02 = 1
        else 
          NumText_02 = 0
      }
      else if (word_close == ")" && NumText_01 == 0 && NumText_02 == 0)
        NumParen -= 1
      else if (word_close == "(" && NumText_01 == 0 && NumText_02 == 0)
        NumParen += 1
    }
    
    //if found close
    if (NumParen == 0) {                
      val temp1_sql = SQL.substring(localWords(position+1).getpos_start, localWords(last_pos_close-1).getpos_end )
      res.select_on_select  = decodeSQL(temp1_sql, TablesAndColumns)
      //new position                    
      res.position = last_pos_close
    }
    
    
    return res
  }
  
  /**
   * close "FROM", return SQL and find database.table information of columns used
   */
  private def Analyze_SQL_CloseFROM (decode_result: huemul_sql_decode_result, TablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns], position_start: Int, position_end: Int, SQL: String ): huemul_sql_decode_result = {
    decode_result.from_sql = SQL.substring(position_start, position_end) 
    
    var textTables: String = ""
    var localTablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns] = new ArrayBuffer[huemul_sql_tables_and_columns]()
    decode_result.tables.foreach { x =>
      
      if (x.database_name == null) {
        val tabRes = TablesAndColumns.filter { y => y.table_name != null && y.table_name.toUpperCase() == x.table_name.toUpperCase()  }  
        if (tabRes.length > 0)
          x.database_name = tabRes(0).database_name
      }
      
      if (x.tableAlias_name == null)
        x.tableAlias_name = x.table_name
      
      val dbandtable = s"[${x.database_name}.${x.table_name}]"
      if (!textTables.contains(dbandtable)){
        localTablesAndColumns = localTablesAndColumns ++ TablesAndColumns.filter { y => y.table_name.toUpperCase() == x.table_name.toUpperCase() && y.database_name.toUpperCase() == x.database_name.toUpperCase()  }  
        textTables = textTables.concat(dbandtable)
      }
      //println(s"database_name: [${x.database_name}], table_Name: [${x.table_name}], alias: [${x.tableAlias_name}]")  
    }
    
    //println ("*** columns")
    decode_result.columns.foreach { x => 
      //println(s"name: [${x.column_name}] sql: [${x.column_sql}] ")
      if (x.column_name == null && x.column_sql == "*") {
        //get "select *, add all columns origin from all tables or alias specify
        localTablesAndColumns.foreach { x_newcolums =>
          val localDB = decode_result.tables.filter { x_tables => x_newcolums.table_name.toUpperCase() == x_tables.table_name.toUpperCase() && x_newcolums.database_name.toUpperCase() == x_tables.database_name.toUpperCase() }
          var localAliasName: String = null
          if (localDB.length > 0) 
            localAliasName = localDB(0).tableAlias_name
            
          val addCol = new huemul_sql_columns()
          addCol.column_name = x_newcolums.column_name
          addCol.column_sql = x_newcolums.column_name.concat(" --added automatically by huemul")
          val addcolumn_origin = new huemul_sql_columns_origin()
          addcolumn_origin.trace_column_name = x_newcolums.column_name
          addcolumn_origin.trace_database_name = x_newcolums.database_name
          addcolumn_origin.trace_table_name = x_newcolums.table_name
          addcolumn_origin.trace_tableAlias_name = if (localAliasName == null) x_newcolums.table_name else localAliasName
          addCol.column_origin.append(addcolumn_origin)
          decode_result.columns.append(addCol)
        }
      } else if (x.column_name == null && x.column_origin.length == 1 && x.column_origin(0).trace_column_name == "*") {
        //get database & table names
        val localDB = decode_result.tables.filter { x_tables => x_tables.tableAlias_name.toUpperCase() == x.column_origin(0).trace_tableAlias_name.toUpperCase()}
        if (localDB.length > 0) {
          //filter table from localtables
          val newColumns = localTablesAndColumns.filter { x_localcols => x_localcols.table_name.toUpperCase() == localDB(0).table_name.toUpperCase() && x_localcols.database_name.toUpperCase() == localDB(0).database_name.toUpperCase() }
          
          newColumns.foreach { x_newcolums =>
            val addCol = new huemul_sql_columns()
            addCol.column_name = x_newcolums.column_name
            addCol.column_sql = x_newcolums.column_name.concat(" --added automatically by huemul")
            val addcolumn_origin = new huemul_sql_columns_origin()
            addcolumn_origin.trace_column_name = x_newcolums.column_name
            addcolumn_origin.trace_database_name = x_newcolums.database_name
            addcolumn_origin.trace_table_name = x_newcolums.table_name
            addcolumn_origin.trace_tableAlias_name = localDB(0).tableAlias_name
            addCol.column_origin.append(addcolumn_origin)
            decode_result.columns.append(addCol) 
          }           
        }
      } else {
      
        x.column_origin.foreach { y_col_orig =>
          //complete database & Table information)
          if (y_col_orig.trace_database_name == null) {          
            //Search for table trace 
            //                                     alias found in "SELECT" sentence              alias of table equal to alias referece in column              
            val RegFound = decode_result.tables.filter { z_tab1 => y_col_orig.trace_tableAlias_name != null  && z_tab1.tableAlias_name.toUpperCase() == y_col_orig.trace_tableAlias_name.toUpperCase() }
            if (RegFound.length == 1) {
              //get tables information of "FROM" sentence
              y_col_orig.trace_table_name = RegFound(0).table_name
              y_col_orig.trace_database_name= RegFound(0).database_name
              y_col_orig.trace_tableAlias_name = RegFound(0).tableAlias_name
            } else if (y_col_orig.trace_tableAlias_name == null) {              
              //if not found with self query, search in param TablesAndColumns (all columns & tables)
              val ResColAndTable = TablesAndColumns.filter { z_table => z_table.column_name != null && z_table.column_name.toUpperCase() ==  y_col_orig.trace_column_name.toUpperCase() }
              
              //search table name of column found in "FROM" table list  
              var isFound: Boolean = false
              ResColAndTable.foreach { z_table2 => 
                val tabFound = decode_result.tables.filter { a_tabfrom => a_tabfrom.table_name.toUpperCase() == z_table2.table_name.toUpperCase() }
                if (tabFound.length == 1){
                  isFound = true
                  y_col_orig.trace_table_name = z_table2.table_name
                  y_col_orig.trace_database_name = z_table2.database_name
                  y_col_orig.trace_tableAlias_name = z_table2.table_name
                }
              }
              
              if (!isFound){
                y_col_orig.trace_table_name = null //UNKNOWN
                y_col_orig.trace_database_name = null //UNKNOWN
              }
            }
          }
         
        }
      }
        
      //println("final")
      //x.column_origin.foreach { y => println(s"[${y.trace_tableAlias_name}].[${y.trace_column_name}]   trace_table_name:${y.trace_table_name} database:${y.trace_database_name}")}
      }
    
    return decode_result
  }
  
   /**
   * close "WHERE", return SQL and find database.table information of columns used IN WHERE
   */
  private def Analyze_SQL_CloseWHERE(decode_result: huemul_sql_decode_result, TablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns], tables: ArrayBuffer[huemul_sql_tables], position_start: Int, position_end: Int, SQL: String ): huemul_sql_decode_result = {
    decode_result.where_sql = SQL.substring(position_start, position_end) 
    
    //filter only tables used in FROM sentence
    var DataFiltered: ArrayBuffer[huemul_sql_tables_and_columns] = new ArrayBuffer[huemul_sql_tables_and_columns]() 
    tables.foreach { x_tablesUsed => 
           DataFiltered = DataFiltered ++
              TablesAndColumns.filter { y_full => y_full.table_name.toUpperCase() == x_tablesUsed.table_name.toUpperCase() && (   (x_tablesUsed.database_name == null)
                                                                                                                               || (x_tablesUsed.database_name.toUpperCase() == y_full.database_name.toUpperCase())
                                                                                                                              )
                                      }
              
        }
    
    //to complete information, use DataFiltered
    decode_result.columns_where.foreach { x_newreg =>
        //complete table and database name with ALIAS name
        if (x_newreg.trace_tableAlias_name != null) {
          val tablefound = tables.filter { x_table => x_table.tableAlias_name.toUpperCase() == x_newreg.trace_tableAlias_name.toUpperCase()  }
          if (tablefound.length > 0) {
            x_newreg.trace_table_name = tablefound(0).table_name
            x_newreg.trace_database_name = tablefound(0).database_name
          } 
        } else {
          //if not found, search in all catalog by column name
          val tableFound = DataFiltered.filter { x_all => x_all.column_name.toUpperCase() == x_newreg.trace_column_name.toUpperCase() }
          if (tableFound.length > 0) {
            x_newreg.trace_table_name = tableFound(0).table_name
            x_newreg.trace_database_name = tableFound(0).database_name
          }
        }
    }
    
    /*  
    println("INICIO********************************")
    DataFiltered.foreach { x => println(s"${x.table_name}, ${x.column_name}") }
    println("FIN********************************")
    
    println("INICIO********************************")
    decode_result.columns_where.foreach { x => println(s"x.trace_column_name:${x.trace_column_name}, x.trace_table_name:${x.trace_table_name}, x.trace_tableAlias_name:${x.trace_tableAlias_name}") }
    println("FIN********************************")
    * 
    */
    
    return decode_result
      
  }
  
  
  private def Analyze_SQL(p_words: ArrayBuffer[huemul_sql_symbol_base], SQL: String, TablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns]): huemul_sql_decode_result = {
    var Result: huemul_sql_decode_result = new huemul_sql_decode_result()
    
    
    var position: Int = 0
   
    var stage: String = null
    var substage: String = null
    var search_end_bool: Boolean = false
    var SentenceIsFound: Boolean = false
    //variables to save select fields
    var column = new huemul_sql_columns()
    var table_from = new huemul_sql_tables()
    var cols_functions: ArrayBuffer[String] = new ArrayBuffer[String]() 
    var position_sql_begin: Int = 0  //save position start and end from fields (Select)
    //var position_end: Int = 0
    var select_on_select: huemul_sql_decode_result = null
    
    //clean "." character, is used as database and columns specification.
    val Symbols_adhoc = Symbols.filter { x => x != "." }
    val search_end_text: ArrayBuffer[String] = new ArrayBuffer[String]() 
    var CanProcess: Boolean = true
    var parenthesis: Int = 0
    var numOfWords: Int = 0
    
    //Drop commented lines with --
    val localWords = Analyze_SQL_RemoveComments(p_words)
    
    
    var actual_line = -1
    position = 0
    var position_max: Int = localWords.length
    while (position < position_max) {
      var word: String = localWords(position).getsymbol.toUpperCase()
      
      //Get current word, previous word and next word
      var word_next: String = null
      var word_prev: String = null
      if (position + 1 < position_max)
        word_next = localWords(position+1).getsymbol.toUpperCase()
      if (position > 0)
        word_prev = localWords(position-1).getsymbol.toUpperCase()
        
      if (CanProcess) {
        //try to determine stage
        if (stage == null) {
          if (word == "SELECT") {
            if (word_next == "DISTINCT")
              position += 1
            stage = "SELECT"
            substage = null
          }
          if (word == "FROM"){
            stage = "FROM"
            position_sql_begin = localWords(position).getpos_start
          }
        } else if (word == "WHERE" && stage == "FROM") {
            stage = "WHERE"
            Result = Analyze_SQL_CloseFROM(Result, TablesAndColumns, position_sql_begin, localWords(position-1).getpos_end, SQL)
            
            position_sql_begin = localWords(position).getpos_start
        } else if (word == "GROUP" && stage == "FROM") {
            stage = "GROUP BY"
            Result = Analyze_SQL_CloseFROM(Result, TablesAndColumns, position_sql_begin, localWords(position-1).getpos_end, SQL)
            
            position_sql_begin = localWords(position).getpos_start
        } else if (word == "ORDER" && stage == "FROM") {
            stage = "ORDER"
            Result = Analyze_SQL_CloseFROM(Result, TablesAndColumns, position_sql_begin, localWords(position-1).getpos_end, SQL)
            
            position_sql_begin = localWords(position).getpos_start
        } else if (word == "GROUP" && stage == "WHERE") {
            stage = "GROUP BY"
            Result = Analyze_SQL_CloseWHERE(Result,TablesAndColumns, Result.tables, position_sql_begin, localWords(position-1).getpos_end, SQL)
            
            position_sql_begin = localWords(position).getpos_start
        } else if (word == "ORDER" && stage == "WHERE") {
            stage = "ORDER"
            Result = Analyze_SQL_CloseWHERE(Result,TablesAndColumns, Result.tables, position_sql_begin, localWords(position-1).getpos_end, SQL)
            
            position_sql_begin = localWords(position).getpos_start
        } else {
          
          //determine substage
          if (stage == "SELECT" && substage == null) {

            column = new huemul_sql_columns()
            var psum = if (word == ",") 1 else 0
            position_sql_begin = localWords(position + psum).getpos_start
            substage = "COLUMN"
            numOfWords = 0
          }
          
          
          //flag off, to determine if sql function or anothers found
          SentenceIsFound = false
          
          /*****************************************************************************************************/
          /***********   R E M O V E   "    ****************************************/
          /*****************************************************************************************************/
          
          //search for end of string sentence like "something" (the second one)
          if (search_end_bool) {
            var pos: Int = -1
            
            //loop for search in array
            breakable {
              for (i <- 0 to search_end_text.length-1) {
                if (search_end_text(i) == word) {
                  pos = i
                  break  
                }
              }
            }
            
            //if found, remove from array and off flag search_end_bool
            if (pos >= 0) {
              SentenceIsFound = true
              search_end_text.remove(pos)
              if (search_end_text.length == 0)
                search_end_bool = false
            }
          } else {
            //search for ""
            val sumbol_key_filter = Symbols_keys.filter { x => x.getsymbol_start.toUpperCase() == word.toUpperCase() && x.getsymbol_isForText  }
            if (sumbol_key_filter.length > 0) {
              SentenceIsFound = true
              
              //if found, add to array
              search_end_text.append(sumbol_key_filter(0).getsymbol_end)
              search_end_bool = true
            }
          }
          /*****************************************************************************************************/
          /***********   S T A R T   S E L E C T   A N D   C O L U M N  ****************************************/
          /*****************************************************************************************************/
          
          //looking for field}
          if (stage == "SELECT" && substage == "COLUMN") {
            //search for "(" and ")"
            if (word == "(")
              parenthesis += 1
            else if (word == ")")
              parenthesis -= 1
                        
            if (word == "(" && word_next == "SELECT") {
              SentenceIsFound = true
              val res_sel = get_select_on_select(position, position_max, SQL, localWords, TablesAndColumns)
              
              position = res_sel.position
              select_on_select = res_sel.select_on_select
              numOfWords += 2
            }
            
            //search for sql functions
            if (SentenceIsFound == false)  {
              val use_function = SQL_Functions.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (use_function.length > 0) {
                SentenceIsFound = true
                cols_functions.append(word)
                numOfWords += 1
              } 
            }
              
            //search for other operations (like +, -, etc)
            if (SentenceIsFound == false)  {
              val _Symbol = Symbols_adhoc.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_Symbol.length > 0) {
                SentenceIsFound = true  
              }
            }
              
            //search for user symbols
            if (SentenceIsFound == false) {
              val _Symbols_user = Symbols_user.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_Symbols_user.length > 0) {
                SentenceIsFound = true
                numOfWords += 1
              }
            }
             
            //end of columns (ex: select  table.field as myField, table.field2 as myfield2 FROM
            //                                                 ---                         ---
            if (search_end_bool == false && position_sql_begin > 0 && (   (word_next == "," && parenthesis == 0) 
                                                                        || word_next == "FROM"
                                                                       )) {
              var colName: String = ""
              if (SentenceIsFound)
                colName = null
              else 
                colName = word
                
              //if word is the column name and alias at the same time (for example "select column from table"), then add column origin
              if (column.column_origin.length == 0 && (  numOfWords == 1 
                                                      || (word_prev == "SELECT")
                                                      || (word_prev == "," && numOfWords == 0)  
                                                      )) {
                val new_reg =  new huemul_sql_columns_origin()
                new_reg.trace_column_name = word
                column.column_origin.append(new_reg)
              }
                
              //Setting all columns
              column.column_name = colName
              column.sql_pos_start = position_sql_begin
              column.sql_pos_end = localWords(position).getpos_end
              column.column_sql = SQL.substring(position_sql_begin, column.sql_pos_end )
              if (select_on_select != null) {
                column.column_origin = select_on_select.columns(0).column_origin
                select_on_select = null
              }
              Result.columns.append(column)
              
              //Clean for next cycle
              substage = null
              if (word_next == "FROM"){
                stage = null
              }
              
            } else if (SentenceIsFound == false && search_end_bool == false && word == ".") {
              val new_reg =  new huemul_sql_columns_origin()
              new_reg.trace_column_name = word_next
              new_reg.trace_tableAlias_name = word_prev
              column.column_origin.append(new_reg)
              numOfWords += 1
            } else if (SentenceIsFound == false && search_end_bool == false && (word != "." && word_next != "." && word_prev != ".") && Try(word.toDouble).isFailure) {
              val new_reg =  new huemul_sql_columns_origin()
              new_reg.trace_column_name = word
              column.column_origin.append(new_reg)
              numOfWords += 1
            }
          } else if (stage == "FROM") {
          /*****************************************************************************************************/
          /***********   S T A R T   F R O M   ****************************************/
          /*****************************************************************************************************/
                        
            if (!search_end_bool) {
              if (word == "(" && word_next == "SELECT") {
                //get subquery info
                val res_sel = get_select_on_select(position, position_max, SQL, localWords, TablesAndColumns)
                
                position = res_sel.position
                
                var l_subquery_word_01: String = null
                var l_subquery_word_02: String = null
                  
                //get next word to 
                if (position+1 < position_max) {
                  l_subquery_word_01 = localWords(position+1).getsymbol.toUpperCase()
                }
                   
                table_from = new huemul_sql_tables()
                table_from.tableAlias_name = l_subquery_word_01

                table_from.database_name = res_sel.select_on_select.AliasDatabase
                table_from.table_name = res_sel.select_on_select.AliasQuery
                
                //table_from.database_name = "TEMP_HUEMUL"
                //table_from.table_name = s"TEMP_HUEMUL_${numTempSubQuery}"
                //numTempSubQuery += 1
                Result.tables.append(table_from)
                Result.subquery_result.append(res_sel.select_on_select)
                
                //Add temp sub query to table list
                res_sel.select_on_select.columns.foreach { x_subquery => 
                  println(s"inserta datos ${table_from.database_name}, ${table_from.table_name}, ${x_subquery.column_name}")
                  val tempQuery = new huemul_sql_tables_and_columns().setData(table_from.database_name, table_from.table_name, x_subquery.column_name)
                  TablesAndColumns.append(tempQuery)
                }
                
                
                
              } else if (word_prev == "FROM" || word_prev == "JOIN" || word_prev == ",") {
                table_from = new huemul_sql_tables()
                //try to get database.table form
                if (word_next == ".") {
                  table_from.database_name = word
                  
                  //Get next position
                  if (position+2 < position_max) {
                    word_prev = localWords(position+1).getsymbol.toUpperCase()
                    word = localWords(position+2).getsymbol.toUpperCase()
                    if (position+3 < position_max) 
                      word_next = localWords(position+3).getsymbol.toUpperCase()
                    else
                      word_next = null
                    
                    table_from.table_name = word
                    position += 2
                  }
                } else {
                  table_from.table_name = word
                }
                
                var word_last: String = null
                if (position+2 < position_max){
                  word_last = localWords(position+2).getsymbol.toUpperCase()
                }
                    
                if (   word_next == "ON" 
                    || word_next == "," 
                    || word_next == "ORDER"
                    || word_next == "GROUP"
                    || word_next == "WHERE"
                    || Symbols_joins.filter { x => word_next != null && x.toUpperCase() == word_next.toUpperCase() }.length > 0
                    ) {
                  table_from.tableAlias_name = table_from.table_name
                  
                } else if (word_next != null && word_next.toUpperCase() == "AS") {
                  table_from.tableAlias_name = word_last
                   if (word_last != null)
                    position += 2
                } else {
                 table_from.tableAlias_name = word_next
                 if (word_last != null) //only add 1
                   position += 1
                }
                   
                Result.tables.append(table_from)
                
              } 
            }              
          } else if (stage == "WHERE") {
            /*****************************************************************************************************/
            /***********   S T A R T   W H E R E   ****************************************/
            /*****************************************************************************************************/
            
            if (word == "(" && word_next == "SELECT") {
              SentenceIsFound = true
              val res_sel = get_select_on_select(position, position_max, SQL, localWords, TablesAndColumns)
              
              position = res_sel.position
              select_on_select = res_sel.select_on_select
            }
            
            //search for sql functions
            if (SentenceIsFound == false)  {
              val use_function = SQL_Functions.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (use_function.length > 0) {
                SentenceIsFound = true
                cols_functions.append(word)
              } 
            }
              
            //search for other operations (like +, -, etc)
            if (SentenceIsFound == false)  {
              val _Symbol = Symbols_adhoc.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_Symbol.length > 0) {
                SentenceIsFound = true  
              }
            }
              
            //search for user symbols
            if (SentenceIsFound == false) {
              val _Symbols_user = Symbols_user.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_Symbols_user.length > 0) {
                SentenceIsFound = true
              }
            }
            
            if (Try(word.toDouble).isSuccess) {
              SentenceIsFound = true
            }
             
            if (SentenceIsFound == false && search_end_bool == false && word == ".") {
              val new_reg =  new huemul_sql_columns_origin()
              new_reg.trace_column_name = word_next
              new_reg.trace_tableAlias_name = word_prev
              Result.columns_where.append(new_reg)
            } else if (SentenceIsFound == false && search_end_bool == false && (word != "." && word_next != "." && word_prev != ".") && Try(word.toDouble).isFailure ) {
              val new_reg =  new huemul_sql_columns_origin()
              new_reg.trace_column_name = word
              Result.columns_where.append(new_reg)
            }
          }
           
          //END SELECT & FROM
        }
      }
      position += 1
    } 
    
    //if last stage was "FROM" --> SQL sentence does't have WHERE, GROUP BY OR HAVING
    if (stage == "FROM") {
      Result = Analyze_SQL_CloseFROM(Result, TablesAndColumns, position_sql_begin, localWords(position-1).getpos_end, SQL)
    } else if (stage == "WHERE") {
      Result = Analyze_SQL_CloseWHERE(Result,TablesAndColumns, Result.tables, position_sql_begin, localWords(position-1).getpos_end, SQL)
    }
    
    return Result
    
  }
  
  def decodeSQL(SQL: String, TablesAndColumns: ArrayBuffer[huemul_sql_tables_and_columns] = new ArrayBuffer[huemul_sql_tables_and_columns] ): huemul_sql_decode_result = {
    
    //split words
    val words = splitWords(SQL)
   
    var i:Int = 0
    var numErrors: Int = 0
    words.foreach { x => 
      //println(s"x.getpos_start: ${x.getpos_start}, x.getpos_end: ${x.getpos_end}")
      
      
      if (x.getsymbol != SQL.substring(x.getpos_start, x.getpos_end)) {
        numErrors += 1
        println(s"ALERT: ERROR READING IN pos: ${i} line: ${x.getsymbol_line}; Valida:${if (x.getsymbol == SQL.substring(x.getpos_start, x.getpos_end)) "true" else "false" } , word: [${x.getsymbol}], largo: ${x.getsymbol.length()}, Pos Start: ${x.getpos_start}, Pos end: ${x.getpos_end}, comprobar: [${SQL.substring(x.getpos_start, x.getpos_end)}] ")        
      }
      
      i += 1
    }

    //Analyze SQL
    val res = Analyze_SQL(words, SQL, TablesAndColumns)
    res.NumErrors = numErrors
    res.AutoIncSubQuery = numTempSubQuery
    numTempSubQuery += 1
    res.AliasQuery = s"TEMP_HUEMUL_${numTempSubQuery}"
    res.AliasDatabase = "TEMP_HUEMUL"
    return res
  }
}