package com.huemulsolutions.bigdata.sql_decode

import scala.collection.mutable._
import util.control.Breaks._
import scala.util.Try

class HuemulSqlDecode(symbolsUser: ArrayBuffer[String], autoIncSubQuery: Int ) extends Serializable {
  symbolsUser.append("AS")
  symbolsUser.append(")")
  symbolsUser.append("(")
  symbolsUser.append("CASE")
  symbolsUser.append("THEN")
  symbolsUser.append("END")
  symbolsUser.append("ELSE")
  symbolsUser.append("FROM")
  symbolsUser.append("WHERE")
  symbolsUser.append("GROUP")
  symbolsUser.append("BY")
  symbolsUser.append("HAVING")
  symbolsUser.append("IS")
  symbolsUser.append("NULL")
  //ADD DataTypes
  symbolsUser.append("INT")
  symbolsUser.append("INTEGER")
  symbolsUser.append("STRING")
  symbolsUser.append("VARCHAR")
  symbolsUser.append("VARCHAR2")
  symbolsUser.append("BOOLEAN")
  symbolsUser.append("LONG")
  symbolsUser.append("REAL")
  symbolsUser.append("FLOAT")
  symbolsUser.append("DATE")
  symbolsUser.append("DATETIME")
  symbolsUser.append("TIMESTAMP")
  symbolsUser.append("TINYINT")
  symbolsUser.append("SMALLINT")

  val symbolsJoins: ArrayBuffer[String] = new ArrayBuffer[String]
  symbolsJoins.append("INNER")
  symbolsJoins.append("LEFT")
  symbolsJoins.append("RIGHT")
  symbolsJoins.append("FULL")
  symbolsJoins.append("JOIN")

  val symbols: ArrayBuffer[String] = new ArrayBuffer[String]
  symbols.append("+")
  symbols.append("-")
  symbols.append("*")
  symbols.append("/")
  symbols.append(",")
  symbols.append(".")
  symbols.append("%")
  symbols.append("=")
  symbols.append(">")
  symbols.append("<")
  symbols.append("!")

  val sqlFunctions: ArrayBuffer[String] = new ArrayBuffer[String]
  sqlFunctions.append("ABS")
  sqlFunctions.append("ACOS")
  sqlFunctions.append("ADD_MONTHS")
  sqlFunctions.append("AND")
  sqlFunctions.append("APPROX_COUNT_DISTINCT")
  sqlFunctions.append("APPROX_PERCENTILE")
  sqlFunctions.append("ARRAY")
  sqlFunctions.append("ARRAY_CONTAINS")
  sqlFunctions.append("ASCII")
  sqlFunctions.append("ASIN")
  sqlFunctions.append("ASSERT_TRUE")
  sqlFunctions.append("ATAN")
  sqlFunctions.append("ATAN2")
  sqlFunctions.append("AVG")
  sqlFunctions.append("BASE64")
  sqlFunctions.append("BIGINT")
  sqlFunctions.append("BIN")
  sqlFunctions.append("BINARY")
  sqlFunctions.append("BIT_LENGTH")
  sqlFunctions.append("BOOLEAN")
  sqlFunctions.append("BROUND")
  sqlFunctions.append("CAST")
  sqlFunctions.append("CBRT")
  sqlFunctions.append("CEIL")
  sqlFunctions.append("CEILING")
  sqlFunctions.append("CHAR")
  sqlFunctions.append("CHAR_LENGTH")
  sqlFunctions.append("CHARACTER_LENGTH")
  sqlFunctions.append("CHR")
  sqlFunctions.append("COALESCE")
  sqlFunctions.append("COLLECT_LIST")
  sqlFunctions.append("COLLECT_SET")
  sqlFunctions.append("CONCAT")
  sqlFunctions.append("CONCAT_WS")
  sqlFunctions.append("CONV")
  sqlFunctions.append("CORR")
  sqlFunctions.append("COS")
  sqlFunctions.append("COSH")
  sqlFunctions.append("COT")
  sqlFunctions.append("COUNT")
  sqlFunctions.append("COUNT_MIN_SKETCH")
  sqlFunctions.append("COVAR_POP")
  sqlFunctions.append("COVAR_SAMP")
  sqlFunctions.append("CRC32")
  sqlFunctions.append("CUBE")
  sqlFunctions.append("CUME_DIST")
  sqlFunctions.append("CURRENT_DATABASE")
  sqlFunctions.append("CURRENT_DATE")
  sqlFunctions.append("CURRENT_TIMESTAMP")
  sqlFunctions.append("DATE")
  sqlFunctions.append("DATE_ADD")
  sqlFunctions.append("DATE_FORMAT")
  sqlFunctions.append("DATE_SUB")
  sqlFunctions.append("DATE_TRUNC")
  sqlFunctions.append("DATEDIFF")
  sqlFunctions.append("DAY")
  sqlFunctions.append("DAYOFMONTH")
  sqlFunctions.append("DAYOFWEEK")
  sqlFunctions.append("DAYOFYEAR")
  sqlFunctions.append("DECIMAL")
  sqlFunctions.append("DECODE")
  sqlFunctions.append("DEGREES")
  sqlFunctions.append("DENSE_RANK")
  sqlFunctions.append("DOUBLE")
  sqlFunctions.append("E")
  sqlFunctions.append("ELT")
  sqlFunctions.append("ENCODE")
  sqlFunctions.append("EXP")
  sqlFunctions.append("EXPLODE")
  sqlFunctions.append("EXPLODE_OUTER")
  sqlFunctions.append("EXPM1")
  sqlFunctions.append("FACTORIAL")
  sqlFunctions.append("FIND_IN_SET")
  sqlFunctions.append("FIRST")
  sqlFunctions.append("FIRST_VALUE")
  sqlFunctions.append("FLOAT")
  sqlFunctions.append("FLOOR")
  sqlFunctions.append("FORMAT_NUMBER")
  sqlFunctions.append("FORMAT_STRING")
  sqlFunctions.append("FROM_JSON")
  sqlFunctions.append("FROM_UNIXTIME")
  sqlFunctions.append("FROM_UTC_TIMESTAMP")
  sqlFunctions.append("GET_JSON_OBJECT")
  sqlFunctions.append("GREATEST")
  sqlFunctions.append("GROUPING")
  sqlFunctions.append("GROUPING_ID")
  sqlFunctions.append("HASH")
  sqlFunctions.append("HEX")
  sqlFunctions.append("HOUR")
  sqlFunctions.append("HYPOT")
  sqlFunctions.append("IF")
  sqlFunctions.append("IFNULL")
  sqlFunctions.append("IN")
  sqlFunctions.append("INITCAP")
  sqlFunctions.append("INLINE")
  sqlFunctions.append("INLINE_OUTER")
  sqlFunctions.append("INPUT_FILE_BLOCK_LENGTH")
  sqlFunctions.append("INPUT_FILE_BLOCK_START")
  sqlFunctions.append("INPUT_FILE_NAME")
  sqlFunctions.append("INSTR")
  sqlFunctions.append("INT")
  sqlFunctions.append("ISNAN")
  sqlFunctions.append("ISNOTNULL")
  sqlFunctions.append("ISNULL")
  sqlFunctions.append("JAVA_METHOD")
  sqlFunctions.append("JSON_TUPLE")
  sqlFunctions.append("KURTOSIS")
  sqlFunctions.append("LAG")
  sqlFunctions.append("LAST")
  sqlFunctions.append("LAST_DAY")
  sqlFunctions.append("LAST_VALUE")
  sqlFunctions.append("LCASE")
  sqlFunctions.append("LEAD")
  sqlFunctions.append("LEAST")
  sqlFunctions.append("LEFT")
  sqlFunctions.append("LENGTH")
  sqlFunctions.append("LEVENSHTEIN")
  sqlFunctions.append("LIKE")
  sqlFunctions.append("LN")
  sqlFunctions.append("LOCATE")
  sqlFunctions.append("LOG")
  sqlFunctions.append("LOG10")
  sqlFunctions.append("LOG1P")
  sqlFunctions.append("LOG2")
  sqlFunctions.append("LOWER")
  sqlFunctions.append("LPAD")
  sqlFunctions.append("LTRIM")
  sqlFunctions.append("MAP")
  sqlFunctions.append("MAP_KEYS")
  sqlFunctions.append("MAP_VALUES")
  sqlFunctions.append("MAX")
  sqlFunctions.append("MD5")
  sqlFunctions.append("MEAN")
  sqlFunctions.append("MIN")
  sqlFunctions.append("MINUTE")
  sqlFunctions.append("MOD")
  sqlFunctions.append("MONOTONICALLY_INCREASING_ID")
  sqlFunctions.append("MONTH")
  sqlFunctions.append("MONTHS_BETWEEN")
  sqlFunctions.append("NAMED_STRUCT")
  sqlFunctions.append("NANVL")
  sqlFunctions.append("NEGATIVE")
  sqlFunctions.append("NEXT_DAY")
  sqlFunctions.append("NOT")
  sqlFunctions.append("NOW")
  sqlFunctions.append("NTILE")
  sqlFunctions.append("NULLIF")
  sqlFunctions.append("NVL")
  sqlFunctions.append("NVL2")
  sqlFunctions.append("OCTET_LENGTH")
  sqlFunctions.append("OR")
  sqlFunctions.append("PARSE_URL")
  sqlFunctions.append("PERCENT_RANK")
  sqlFunctions.append("PERCENTILE")
  sqlFunctions.append("PERCENTILE_APPROX")
  sqlFunctions.append("PI")
  sqlFunctions.append("PMOD")
  sqlFunctions.append("POSEXPLODE")
  sqlFunctions.append("POSEXPLODE_OUTER")
  sqlFunctions.append("POSITION")
  sqlFunctions.append("POSITIVE")
  sqlFunctions.append("POW")
  sqlFunctions.append("POWER")
  sqlFunctions.append("PRINTF")
  sqlFunctions.append("QUARTER")
  sqlFunctions.append("RADIANS")
  sqlFunctions.append("RAND")
  sqlFunctions.append("RANDN")
  sqlFunctions.append("RANK")
  sqlFunctions.append("REFLECT")
  sqlFunctions.append("REGEXP_EXTRACT")
  sqlFunctions.append("REGEXP_REPLACE")
  sqlFunctions.append("REPEAT")
  sqlFunctions.append("REPLACE")
  sqlFunctions.append("REVERSE")
  sqlFunctions.append("RIGHT")
  sqlFunctions.append("RINT")
  sqlFunctions.append("RLIKE")
  sqlFunctions.append("ROLLUP")
  sqlFunctions.append("ROUND")
  sqlFunctions.append("ROW_NUMBER")
  sqlFunctions.append("RPAD")
  sqlFunctions.append("RTRIM")
  sqlFunctions.append("SECOND")
  sqlFunctions.append("SENTENCES")
  sqlFunctions.append("SHA")
  sqlFunctions.append("SHA1")
  sqlFunctions.append("SHA2")
  sqlFunctions.append("SHIFTLEFT")
  sqlFunctions.append("SHIFTRIGHT")
  sqlFunctions.append("SHIFTRIGHTUNSIGNED")
  sqlFunctions.append("SIGN")
  sqlFunctions.append("SIGNUM")
  sqlFunctions.append("SIN")
  sqlFunctions.append("SINH")
  sqlFunctions.append("SIZE")
  sqlFunctions.append("SKEWNESS")
  sqlFunctions.append("SMALLINT")
  sqlFunctions.append("SORT_ARRAY")
  sqlFunctions.append("SOUNDEX")
  sqlFunctions.append("SPACE")
  sqlFunctions.append("SPARK_PARTITION_ID")
  sqlFunctions.append("SPLIT")
  sqlFunctions.append("SQRT")
  sqlFunctions.append("STACK")
  sqlFunctions.append("STD")
  sqlFunctions.append("STDDEV")
  sqlFunctions.append("STDDEV_POP")
  sqlFunctions.append("STDDEV_SAMP")
  sqlFunctions.append("STR_TO_MAP")
  sqlFunctions.append("STRING")
  sqlFunctions.append("STRUCT")
  sqlFunctions.append("SUBSTR")
  sqlFunctions.append("SUBSTRING")
  sqlFunctions.append("SUBSTRING_INDEX")
  sqlFunctions.append("SUM")
  sqlFunctions.append("TAN")
  sqlFunctions.append("TANH")
  sqlFunctions.append("TIMESTAMP")
  sqlFunctions.append("TINYINT")
  sqlFunctions.append("TO_DATE")
  sqlFunctions.append("TO_JSON")
  sqlFunctions.append("TO_TIMESTAMP")
  sqlFunctions.append("TO_UNIX_TIMESTAMP")
  sqlFunctions.append("TO_UTC_TIMESTAMP")
  sqlFunctions.append("TRANSLATE")
  sqlFunctions.append("TRIM")
  sqlFunctions.append("TRUNC")
  sqlFunctions.append("UCASE")
  sqlFunctions.append("UNBASE64")
  sqlFunctions.append("UNHEX")
  sqlFunctions.append("UNIX_TIMESTAMP")
  sqlFunctions.append("UPPER")
  sqlFunctions.append("UUID")
  sqlFunctions.append("VAR_POP")
  sqlFunctions.append("VAR_SAMP")
  sqlFunctions.append("VARIANCE")
  sqlFunctions.append("WEEKOFYEAR")
  sqlFunctions.append("WHEN")
  sqlFunctions.append("WINDOW")
  sqlFunctions.append("XPATH")
  sqlFunctions.append("XPATH_BOOLEAN")
  sqlFunctions.append("XPATH_DOUBLE")
  sqlFunctions.append("XPATH_FLOAT")
  sqlFunctions.append("XPATH_INT")
  sqlFunctions.append("XPATH_LONG")
  sqlFunctions.append("XPATH_NUMBER")
  sqlFunctions.append("XPATH_SHORT")
  sqlFunctions.append("XPATH_STRING")
  sqlFunctions.append("YEAR")

  val symbolsKeys: ArrayBuffer[HuemulSqlSymbol] = new ArrayBuffer[HuemulSqlSymbol]
  symbolsKeys.append(new HuemulSqlSymbol("(",")",false))
  symbolsKeys.append(new HuemulSqlSymbol("'","'",true))
  symbolsKeys.append(new HuemulSqlSymbol(""""""",""""""",true))

  val Symbols_drops: ArrayBuffer[String] = new ArrayBuffer[String]
  Symbols_drops.append("\n")

  var line: Int = 0
  private var numTempSubQuery: Int = autoIncSubQuery
  def getNumTempSubQuery: Int = numTempSubQuery

  /**
   * split a string in characters, search for "\n" and others for split in two o more words
   */
  private def splitKeys(text: String, posRealStart: Int, posRealEnd: Int): ArrayBuffer[HuemulSqlSymbolBase] = {
    val result: ArrayBuffer[HuemulSqlSymbolBase] = new ArrayBuffer[HuemulSqlSymbolBase] ()
    var restOfText: String = text
    var positionWord: Integer = 0
    //var cycle: Int = 0
    var textFound: Boolean = false
    var textFoundSave: Boolean = true
    var addLine: Boolean = false
    val enter: String = "\n"

    var posStart: Int = posRealStart
    val posEnd: Int = posRealEnd
    while (positionWord < restOfText.length()) {
      textFoundSave = true
      addLine = false
      textFound = false

      if (symbols.exists { x => x(0) == restOfText(positionWord) })
        textFound = true
      else if (symbolsKeys.exists { x => x.getSymbolStart(0) == restOfText(positionWord) || x.getSymbolEnd(0) == restOfText(positionWord) })
        textFound = true
      else if (restOfText(positionWord) == enter(0) || restOfText(positionWord).toInt == 13) {
        textFound = true
        textFoundSave = false
        addLine = true
      }

      //split text if found special characters
      if (textFound) {
        val textBefore = restOfText.substring(0,positionWord)
        val textChar = restOfText.substring(positionWord,positionWord+1)
        restOfText = restOfText.substring(positionWord+1,restOfText.length())
        //Only insert text_before if pos > 0, example  [*campo5] only insert [*] and rest of text [campo5] is for next iteration
        if (positionWord > 0){
          result.append(new HuemulSqlSymbolBase(textBefore, line, posStart, posStart+positionWord))
          posStart = posStart+positionWord
        }

        if (textFoundSave) {
          result.append(new HuemulSqlSymbolBase(textChar, line, posStart, posStart+1))
          posStart = posStart+1
        }

        positionWord = 0
      } else
        positionWord += 1

      if (addLine) {
        line += 1
        posStart += 1
      }
    }

    if (restOfText.nonEmpty)
      result.append(new HuemulSqlSymbolBase(restOfText, line, posStart,posEnd ))

    result
  }

  /**
   * split a text in words, and return only valid words (without spaces and line return)
   */
  private def splitWords(text: String): ArrayBuffer[HuemulSqlSymbolBase] = {
    val Result: ArrayBuffer[HuemulSqlSymbolBase] = new ArrayBuffer[HuemulSqlSymbolBase] ()
    val enter: String = "\n"
    var restOfText: String = text
    //println(rest_of_text)
    //Get first word
    var positionEnd: Integer = 0
    var word: String = null
    var cycle: Int = 0
    line = 1
    var positionRealStart: Int = 0
    var positionRealEnd: Int = 0
    while (positionEnd >= 0 && cycle <= 20000) {
      positionEnd = restOfText.indexOf(" ")
      positionRealEnd = positionRealStart + positionEnd
      if (positionEnd >= 0) {
        word = restOfText.substring(0,positionEnd)
        restOfText = restOfText.substring(positionEnd+1, restOfText.length())
      } else {
        word = restOfText
        positionRealEnd += restOfText.length()+1
        restOfText = null

      }

      if (word != null && word.trim() != "") {
        //split word, searching new characters
        val NewResult = splitKeys(word, positionRealStart, positionRealEnd)
        Result.appendAll(NewResult)
      } else if (word == enter || (word.nonEmpty && word.charAt(0).toInt == 13))
        line += 1

      //println(s"line: $line pos: [$position_end], word: [$word], word len: [${word.length()}]  real ini: $position_real_start, real end: $position_real_end")

      positionRealStart = positionRealEnd + 1

      cycle += 1
    }


    Result
  }

  /**
   * remove comments (like --),
   */
  def analyzeSqlRemoveComments(p_words: ArrayBuffer[HuemulSqlSymbolBase]): ArrayBuffer[HuemulSqlSymbolBase] = {
    val localWords = p_words
    var actualLine = -1
    var position = 0
    var removeStartPos: Int = -1
    while (position < localWords.length-1) {
      //doesn't have commented lines
      if (actualLine == -1) {
        if (localWords(position).getSymbol == "-" && localWords(position+1).getSymbol == "-") {
          actualLine = localWords(position).getSymbolLine
          removeStartPos = position
        }
      } else {
        if (actualLine != localWords(position).getSymbolLine) {
          //Remove lines
          localWords.remove(removeStartPos, position - removeStartPos )

          //set variables to re-start
          actualLine = -1
          position = removeStartPos - 1
        }
      }

      position += 1
    }

    localWords
  }

  /**
   * type for return result of get_select_on_select method
   */
  private class classGetSelectOnSelect {
    var position: Int = 0
    var selectOnSelect: HuemulSqlDecodeResult = _
  }

  /**
   * find end of subquerys
   */
  private def getSelectOnSelect(position: Int, positionMax: Int, SQL: String, localWords: ArrayBuffer[HuemulSqlSymbolBase], TablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns]): classGetSelectOnSelect =  {
    //SELECT INSIDE SELECT, GET COLUMN AND TABLES
    val res = new classGetSelectOnSelect()

    var lastPosClose: Int = position + 1
    var numParen: Int = 1
    var numText01: Int = 0
    var numText02: Int = 0

    //search last position to find close ")"
    while (lastPosClose < positionMax && numParen > 0 ) {
      lastPosClose  += 1
      val wordClose = localWords(lastPosClose).getSymbol.toUpperCase()

      if (wordClose == "'") {
        if (numText01 == 0)
          numText01 = 1
        else
          numText01 = 0
      }
      else if (wordClose == """"""") {
        if (numText02 == 0)
          numText02 = 1
        else
          numText02 = 0
      }
      else if (wordClose == ")" && numText01 == 0 && numText02 == 0)
        numParen -= 1
      else if (wordClose == "(" && numText01 == 0 && numText02 == 0)
        numParen += 1
    }

    //if found close
    if (numParen == 0) {
      val temp1Sql = SQL.substring(localWords(position+1).getPosStart, localWords(lastPosClose-1).getPosEnd )
      res.selectOnSelect  = decodeSql(temp1Sql, TablesAndColumns)
      //new position
      res.position = lastPosClose
    }


    res
  }

  /**
   * close "FROM", return SQL and find database.table information of columns used
   */
  private def analyzeSqlCloseFrom (decodeResult: HuemulSqlDecodeResult, TablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns], position_start: Int, position_end: Int, SQL: String ): HuemulSqlDecodeResult = {
    decodeResult.fromSql = SQL.substring(position_start, position_end)

    var textTables: String = ""
    var localTablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns] = new ArrayBuffer[HuemulSqlTablesAndColumns]()
    decodeResult.tables.foreach { x =>

      if (x.databaseName == null) {
        val tabRes = TablesAndColumns.filter { y => y.tableName != null && y.tableName.toUpperCase() == x.tableName.toUpperCase()  }
        if (tabRes.nonEmpty)
          x.databaseName = tabRes(0).databaseName
      }

      if (x.tableAliasName == null)
        x.tableAliasName = x.tableName

      val dbandtable = s"[${x.databaseName}.${x.tableName}]"
      if (!textTables.contains(dbandtable)){
        localTablesAndColumns = localTablesAndColumns ++ TablesAndColumns.filter { y => y.tableName.toUpperCase() == x.tableName.toUpperCase() && y.databaseName.toUpperCase() == x.databaseName.toUpperCase()  }
        textTables = textTables.concat(dbandtable)
      }
      //println(s"database_name: [${x.database_name}], table_Name: [${x.table_name}], alias: [${x.tableAlias_name}] [${x.tableAlias_name.length()} (${x.tableAlias_name.charAt(x.tableAlias_name.length()-1).toInt})]")
    }

    //println ("*** columns")
    decodeResult.columns.foreach { x =>
      //println(s"name: [${x.column_name}] sql: [${x.column_sql}] ")
      if (x.columnName == null && x.columnSql == "*") {
        //get "select *, add all columns origin from all tables or alias specify
        decodeResult.tables.foreach { x_tables =>
            localTablesAndColumns.filter { x_newColumn => x_newColumn.tableName.toUpperCase() == x_tables.tableName.toUpperCase() && x_newColumn.databaseName.toUpperCase() == x_tables.databaseName.toUpperCase() }.foreach { x_newColumn =>
              //Add all columns of table
              val addCol = new HuemulSqlColumns()
              addCol.columnName = x_newColumn.columnName
              addCol.columnSql = s"${x_tables.tableAliasName}.${x_newColumn.columnName} --added automatically by huemul_sql_decode"
              val addColumnOrigin = new HuemulSqlColumnsOrigin()
              addColumnOrigin.traceColumnName = x_newColumn.columnName
              addColumnOrigin.traceDatabaseName = x_newColumn.databaseName
              addColumnOrigin.traceTableName = x_newColumn.tableName
              addColumnOrigin.traceTableAliasName = x_tables.tableAliasName
              addCol.columnOrigin.append(addColumnOrigin)
              decodeResult.columns.append(addCol)
            }
        }

      } else if (x.columnName == null && x.columnOrigin.length == 1 && x.columnOrigin(0).traceColumnName == "*") {
        //get database & table names
        val localDB = decodeResult.tables.filter { x_tables => x_tables.tableAliasName.toUpperCase() == x.columnOrigin(0).traceTableAliasName.toUpperCase()}
        if (localDB.nonEmpty) {
          //filter table from localtables
          val newColumns = localTablesAndColumns.filter { x_localcols => x_localcols.tableName.toUpperCase() == localDB(0).tableName.toUpperCase() && x_localcols.databaseName.toUpperCase() == localDB(0).databaseName.toUpperCase() }

          newColumns.foreach { x_newColums =>
            val addCol = new HuemulSqlColumns()
            addCol.columnName = x_newColums.columnName
            addCol.columnSql = x_newColums.columnName.concat(" --added automatically by huemul")
            val addColumnOrigin = new HuemulSqlColumnsOrigin()
            addColumnOrigin.traceColumnName = x_newColums.columnName
            addColumnOrigin.traceDatabaseName = x_newColums.databaseName
            addColumnOrigin.traceTableName = x_newColums.tableName
            addColumnOrigin.traceTableAliasName = localDB(0).tableAliasName
            addCol.columnOrigin.append(addColumnOrigin)
            decodeResult.columns.append(addCol)
          }
        }
      } else {

        x.columnOrigin.foreach { yColOrig =>
          //complete database & Table information)
          if (yColOrig.traceDatabaseName == null) {
            //Search for table trace
            //                                     alias found in "SELECT" sentence              alias of table equal to alias referece in column
            val RegFound = decodeResult.tables.filter { z_tab1 => yColOrig.traceTableAliasName != null  && z_tab1.tableAliasName.toUpperCase() == yColOrig.traceTableAliasName.toUpperCase() }
            if (RegFound.length == 1) {
              //get tables information of "FROM" sentence
              yColOrig.traceTableName = RegFound(0).tableName
              yColOrig.traceDatabaseName= RegFound(0).databaseName
              //y_col_orig.trace_tableAlias_name = RegFound(0).tableAlias_name
            } else if (yColOrig.traceTableAliasName == null) {
              //if not found with self query, search in param TablesAndColumns (all columns & tables)
              val ResColAndTable = TablesAndColumns.filter { z_table => z_table.columnName != null && z_table.columnName.toUpperCase() ==  yColOrig.traceColumnName.toUpperCase() }

              //search table name of column found in "FROM" table list
              var isFound: Boolean = false
              ResColAndTable.foreach { z_table2 =>
                val tabFound = decodeResult.tables.filter { aTabFrom => aTabFrom.tableName.toUpperCase() == z_table2.tableName.toUpperCase() }
                if (tabFound.length == 1){
                  isFound = true
                  yColOrig.traceTableName = z_table2.tableName
                  yColOrig.traceDatabaseName = z_table2.databaseName
                  yColOrig.traceTableAliasName = z_table2.tableName
                }
              }

              if (!isFound){
                yColOrig.traceTableName = null //UNKNOWN
                yColOrig.traceDatabaseName = null //UNKNOWN
              }
            }
          }

        }
      }


      //distinct
      val finalOrigin: ArrayBuffer[HuemulSqlColumnsOrigin] = new ArrayBuffer[HuemulSqlColumnsOrigin]()
      x.columnOrigin.foreach { xOriFrom =>
        if (!finalOrigin.exists { xFin =>
          xFin.traceDatabaseName == xOriFrom.traceDatabaseName &&
            xFin.traceTableName == xOriFrom.traceTableName &&
            xFin.traceColumnName == xOriFrom.traceColumnName &&
            xFin.traceTableAliasName == xOriFrom.traceTableAliasName
        }) {
          finalOrigin.append(xOriFrom)
        }
      }



      x.columnOrigin = finalOrigin

      //println("final")
      //x.column_origin.foreach { y => println(s"[${y.trace_tableAlias_name}].[${y.trace_column_name}]   trace_table_name:${y.trace_table_name} database:${y.trace_database_name}")}
    }

    //Exclude select "*"
    decodeResult.columns = decodeResult.columns.filter { x => (
                                                                       !(x.columnName == null && x.columnSql == "*")
                                                                    && !(x.columnName == null && x.columnOrigin.length == 1 && x.columnOrigin(0).traceColumnName == "*")
                                                                  )}

    decodeResult
  }

   /**
   * close "WHERE", return SQL and find database.table information of columns used IN WHERE
   */
  private def analyzeSqlCloseWhere(decodeResult: HuemulSqlDecodeResult, TablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns], tables: ArrayBuffer[HuemulSqlTables], position_start: Int, position_end: Int, SQL: String ): HuemulSqlDecodeResult = {
    decodeResult.whereSql = SQL.substring(position_start, position_end)

    //filter only tables used in FROM sentence
    var DataFiltered: ArrayBuffer[HuemulSqlTablesAndColumns] = new ArrayBuffer[HuemulSqlTablesAndColumns]()
    tables.foreach { x_tablesUsed =>
           DataFiltered = DataFiltered ++
              TablesAndColumns.filter { y_full => y_full.tableName.toUpperCase() == x_tablesUsed.tableName.toUpperCase() && (   (x_tablesUsed.databaseName == null)
                                                                                                                               || (x_tablesUsed.databaseName.toUpperCase() == y_full.databaseName.toUpperCase())
                                                                                                                              )
                                      }

        }

    //to complete information, use DataFiltered
    decodeResult.columnsWhere.foreach { xNewReg =>
        //complete table and database name with ALIAS name
        if (xNewReg.traceTableAliasName != null) {
          val tableFound = tables.filter { x_table => x_table.tableAliasName.toUpperCase() == xNewReg.traceTableAliasName.toUpperCase()  }
          if (tableFound.nonEmpty) {
            xNewReg.traceTableName = tableFound(0).tableName
            xNewReg.traceDatabaseName = tableFound(0).databaseName
          }
        } else {
          //if not found, search in all catalog by column name
          val tableFound = DataFiltered.filter { x_all => x_all.columnName.toUpperCase() == xNewReg.traceColumnName.toUpperCase() }
          if (tableFound.nonEmpty) {
            xNewReg.traceTableName = tableFound(0).tableName
            xNewReg.traceDatabaseName = tableFound(0).databaseName
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

    decodeResult

  }


  private def analyzeSql(p_words: ArrayBuffer[HuemulSqlSymbolBase], SQL: String, TablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns]): HuemulSqlDecodeResult = {
    var result: HuemulSqlDecodeResult = new HuemulSqlDecodeResult()


    var position: Int = 0

    var stage: String = null
    var subStage: String = null
    var searchEndBool: Boolean = false
    var sentenceIsFound: Boolean = false
    //variables to save select fields
    var column = new HuemulSqlColumns()
    var tableFrom = new HuemulSqlTables()
    val colsFunctions: ArrayBuffer[String] = new ArrayBuffer[String]()
    var positionSqlBegin: Int = 0  //save position start and end from fields (Select)
    //var position_end: Int = 0
    var selectOnSelect: HuemulSqlDecodeResult = null

    //clean "." character, is used as database and columns specification.
    val symbolsAdHoc = symbols.filter { x => x != "." }
    val searchEndText: ArrayBuffer[String] = new ArrayBuffer[String]()
    val canProcess: Boolean = true
    var parenthesis: Int = 0
    var numOfWords: Int = 0

    //Drop commented lines with --
    val localWords = analyzeSqlRemoveComments(p_words)


    //var actual_line = -1
    position = 0
    val positionMax: Int = localWords.length
    while (position < positionMax) {
      var word: String = localWords(position).getSymbol.toUpperCase()

      //Get current word, previous word and next word
      var wordNext: String = null
      var wordPrev: String = null
      if (position + 1 < positionMax)
        wordNext = localWords(position+1).getSymbol.toUpperCase()
      if (position > 0)
        wordPrev = localWords(position-1).getSymbol.toUpperCase()


      if (canProcess) {
        //try to determine stage
        if (stage == null) {
          if (word == "SELECT") {
            if (wordNext == "DISTINCT")
              position += 1
            stage = "SELECT"
            subStage = null
          }
          if (word == "FROM"){
            stage = "FROM"
            positionSqlBegin = localWords(position).getPosStart
          }
        } else if (word == "WHERE" && stage == "FROM") {
            stage = "WHERE"
            result = analyzeSqlCloseFrom(result, TablesAndColumns, positionSqlBegin, localWords(position-1).getPosEnd, SQL)

            positionSqlBegin = localWords(position).getPosStart
        } else if (word == "GROUP" && stage == "FROM") {
            stage = "GROUP BY"
            result = analyzeSqlCloseFrom(result, TablesAndColumns, positionSqlBegin, localWords(position-1).getPosEnd, SQL)

            positionSqlBegin = localWords(position).getPosStart
        } else if (word == "ORDER" && stage == "FROM") {
            stage = "ORDER"
            result = analyzeSqlCloseFrom(result, TablesAndColumns, positionSqlBegin, localWords(position-1).getPosEnd, SQL)

            positionSqlBegin = localWords(position).getPosStart
        } else if (word == "GROUP" && stage == "WHERE") {
            stage = "GROUP BY"
            result = analyzeSqlCloseWhere(result,TablesAndColumns, result.tables, positionSqlBegin, localWords(position-1).getPosEnd, SQL)

            positionSqlBegin = localWords(position).getPosStart
        } else if (word == "ORDER" && stage == "WHERE") {
            stage = "ORDER"
            result = analyzeSqlCloseWhere(result,TablesAndColumns, result.tables, positionSqlBegin, localWords(position-1).getPosEnd, SQL)

            positionSqlBegin = localWords(position).getPosStart
        } else {

          //determine substage
          if (stage == "SELECT" && subStage == null) {

            column = new HuemulSqlColumns()
            val pSum = if (word == ",") 1 else 0
            positionSqlBegin = localWords(position + pSum).getPosStart
            subStage = "COLUMN"
            numOfWords = 0
          }


          //flag off, to determine if sql function or anothers found
          sentenceIsFound = false

          /*****************************************************************************************************/
          /***********   R E M O V E   "    ****************************************/
          /*****************************************************************************************************/

          //search for end of string sentence like "something" (the second one)
          if (searchEndBool) {
            var pos: Int = -1

            //loop for search in array
            breakable {
              for (i <- searchEndText.indices) {
                if (searchEndText(i) == word) {
                  pos = i
                  break
                }
              }
            }

            //if found, remove from array and off flag search_end_bool
            if (pos >= 0) {
              sentenceIsFound = true
              searchEndText.remove(pos)
              if (searchEndText.isEmpty)
                searchEndBool = false
            }
          } else {
            //search for ""
            val sumBolKeyFilter = symbolsKeys.filter { x => x.getSymbolStart.toUpperCase() == word.toUpperCase() && x.getSymbolIsForText  }
            if (sumBolKeyFilter.nonEmpty) {
              sentenceIsFound = true

              //if found, add to array
              searchEndText.append(sumBolKeyFilter(0).getSymbolEnd)
              searchEndBool = true
            }
          }
          /*****************************************************************************************************/
          /***********   S T A R T   S E L E C T   A N D   C O L U M N  ****************************************/
          /*****************************************************************************************************/

          //looking for field}
          if (stage == "SELECT" && subStage == "COLUMN") {
            //search for "(" and ")"


            if (word == "(" && wordNext == "SELECT") {
              sentenceIsFound = true
              val res_sel = getSelectOnSelect(position, positionMax, SQL, localWords, TablesAndColumns)

              position = res_sel.position
              selectOnSelect = res_sel.selectOnSelect
              numOfWords += 2
            } else if (word == "(")
              parenthesis += 1
            else if (word == ")")
              parenthesis -= 1

            //search for sql functions
            if (!sentenceIsFound)  {
              val use_function = sqlFunctions.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (use_function.nonEmpty) {
                sentenceIsFound = true
                colsFunctions.append(word)
                numOfWords += 1
              }
            }

            //search for other operations (like +, -, etc)
            if (!sentenceIsFound)  {
              val _Symbol = symbolsAdHoc.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_Symbol.nonEmpty) {
                sentenceIsFound = true
              }
            }

            //search for user symbols
            if (!sentenceIsFound) {
              val _symbolsUser = symbolsUser.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_symbolsUser.nonEmpty) {
                sentenceIsFound = true
                numOfWords += 1
              }
            }

            //end of columns (ex: select  table.field as myField, table.field2 as myfield2 FROM
            //                                                 ---                         ---
            if (!searchEndBool && positionSqlBegin > 0 && (   (wordNext == "," && parenthesis == 0)
                                                                        || wordNext == "FROM"
                                                                       )) {
              var colName: String = ""
              if (sentenceIsFound)
                colName = null
              else
                colName = word

              //if word is the column name and alias at the same time (for example "select column from table"), then add column origin
              if (column.columnOrigin.isEmpty && Try(word.toDouble).isFailure &&
                                       (   numOfWords == 1
                                    || (wordPrev == "SELECT")
                                    || (wordPrev == "," && numOfWords == 0)
                                    )) {
                val newReg =  new HuemulSqlColumnsOrigin()
                newReg.traceColumnName = word
                column.columnOrigin.append(newReg)
              }

              //Setting all columns
              column.columnName = colName
              column.sqlPosStart = positionSqlBegin
              column.sqlPosEnd = localWords(position).getPosEnd
              column.columnSql = SQL.substring(positionSqlBegin, column.sqlPosEnd )
              if (selectOnSelect != null) {
                column.columnOrigin = selectOnSelect.columns(0).columnOrigin
                selectOnSelect = null
              }
              result.columns.append(column)

              //Clean for next cycle
              subStage = null
              if (wordNext == "FROM"){
                stage = null
              }

            } else if (!sentenceIsFound && !searchEndBool && word == ".") {
              val newReg =  new HuemulSqlColumnsOrigin()
              newReg.traceColumnName = wordNext
              newReg.traceTableAliasName = wordPrev
              column.columnOrigin.append(newReg)
              numOfWords += 1
            } else if (!sentenceIsFound && !searchEndBool && (word != "." && wordNext != "." && wordPrev != ".") && Try(word.toDouble).isFailure) {
              val newReg =  new HuemulSqlColumnsOrigin()
              newReg.traceColumnName = word
              column.columnOrigin.append(newReg)
              numOfWords += 1
            }
          } else if (stage == "FROM") {
          /*****************************************************************************************************/
          /***********   S T A R T   F R O M   ****************************************/
          /*****************************************************************************************************/

            if (!searchEndBool) {
              if (word == "(" && wordNext == "SELECT") {
                //get subquery info
                val resSel = getSelectOnSelect(position, positionMax, SQL, localWords, TablesAndColumns)

                position = resSel.position

                var l_subquery_word_01: String = null
                //var l_subquery_word_02: String = null

                //get next word to
                if (position+1 < positionMax) {
                  l_subquery_word_01 = localWords(position+1).getSymbol.toUpperCase()
                }

                tableFrom = new HuemulSqlTables()
                tableFrom.tableAliasName = l_subquery_word_01

                tableFrom.databaseName = resSel.selectOnSelect.aliasDatabase
                tableFrom.tableName = resSel.selectOnSelect.aliasQuery

                //table_from.database_name = "TEMP_HUEMUL"
                //table_from.table_name = s"TEMP_HUEMUL_${numTempSubQuery}"
                //numTempSubQuery += 1
                result.tables.append(tableFrom)
                result.subQueryResult.append(resSel.selectOnSelect)

                //Add temp sub query to table list
                resSel.selectOnSelect.columns.foreach { x_subquery =>
                  //println(s"inserta datos ${table_from.database_name}, ${table_from.table_name}, ${x_subquery.column_name}")
                  val tempQuery = new HuemulSqlTablesAndColumns().setData(tableFrom.databaseName, tableFrom.tableName, x_subquery.columnName)
                  TablesAndColumns.append(tempQuery)
                }



              } else if (wordPrev == "FROM" || wordPrev == "JOIN" || wordPrev == ",") {
                tableFrom = new HuemulSqlTables()
                //try to get database.table form
                if (wordNext == ".") {
                  tableFrom.databaseName = word

                  //Get next position
                  if (position+2 < positionMax) {
                    wordPrev = localWords(position+1).getSymbol.toUpperCase()
                    word = localWords(position+2).getSymbol.toUpperCase()
                    if (position+3 < positionMax)
                      wordNext = localWords(position+3).getSymbol.toUpperCase()
                    else
                      wordNext = null

                    tableFrom.tableName = word
                    position += 2
                  }
                } else {
                  tableFrom.tableName = word
                }

                var word_last: String = null
                if (position+2 < positionMax){
                  word_last = localWords(position+2).getSymbol.toUpperCase()
                }

                if (   wordNext == "ON"
                    || wordNext == ","
                    || wordNext == "ORDER"
                    || wordNext == "GROUP"
                    || wordNext == "WHERE"
                    || symbolsJoins.exists { x => wordNext != null && x.toUpperCase() == wordNext.toUpperCase() }
                    ) {
                  tableFrom.tableAliasName = tableFrom.tableName

                } else if (wordNext != null && wordNext.toUpperCase() == "AS") {
                  tableFrom.tableAliasName = word_last
                   if (word_last != null)
                    position += 2
                } else {
                 tableFrom.tableAliasName = wordNext
                 if (word_last != null) //only add 1
                   position += 1
                }

                result.tables.append(tableFrom)

              }
            }
          } else if (stage == "WHERE") {
            /*****************************************************************************************************/
            /***********   S T A R T   W H E R E   ****************************************/
            /*****************************************************************************************************/

            if (word == "(" && wordNext == "SELECT") {
              sentenceIsFound = true
              val res_sel = getSelectOnSelect(position, positionMax, SQL, localWords, TablesAndColumns)

              position = res_sel.position
              selectOnSelect = res_sel.selectOnSelect
            }

            //search for sql functions
            if (!sentenceIsFound)  {
              val useFunction = sqlFunctions.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (useFunction.nonEmpty) {
                sentenceIsFound = true
                colsFunctions.append(word)
              }
            }

            //search for other operations (like +, -, etc)
            if (!sentenceIsFound)  {
              val _symbol = symbolsAdHoc.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_symbol.nonEmpty) {
                sentenceIsFound = true
              }
            }

            //search for user symbols
            if (!sentenceIsFound) {
              val _symbolsUser = symbolsUser.filter { x => x.toUpperCase() == word.toUpperCase() }
              if (_symbolsUser.nonEmpty) {
                sentenceIsFound = true
              }
            }

            if (Try(word.toDouble).isSuccess) {
              sentenceIsFound = true
            }

            if (!sentenceIsFound && !searchEndBool && word == ".") {
              val newReg =  new HuemulSqlColumnsOrigin()
              newReg.traceColumnName = wordNext
              newReg.traceTableAliasName = wordPrev
              result.columnsWhere.append(newReg)
            } else if (!sentenceIsFound && !searchEndBool && (word != "." && wordNext != "." && wordPrev != ".") && Try(word.toDouble).isFailure ) {
              val newReg =  new HuemulSqlColumnsOrigin()
              newReg.traceColumnName = word
              result.columnsWhere.append(newReg)
            }
          }

          //END SELECT & FROM
        }
      }
      position += 1
    }

    //if last stage was "FROM" --> SQL sentence does't have WHERE, GROUP BY OR HAVING
    if (stage == "FROM") {
      result = analyzeSqlCloseFrom(result, TablesAndColumns, positionSqlBegin, localWords(position-1).getPosEnd, SQL)
    } else if (stage == "WHERE") {
      result = analyzeSqlCloseWhere(result,TablesAndColumns, result.tables, positionSqlBegin, localWords(position-1).getPosEnd, SQL)
    }

    result

  }

  /**
   * Decode SQL
   * @param sql SQL code
   * @param tablesAndColumns list of tables and columns
   * @return
   */
  def decodeSql(sql: String, tablesAndColumns: ArrayBuffer[HuemulSqlTablesAndColumns] = new ArrayBuffer[HuemulSqlTablesAndColumns] ): HuemulSqlDecodeResult = {

    //split words
    val words = splitWords(sql)

    var i:Int = 0
    var numErrors: Int = 0
    words.foreach { x =>
      //println(s"x.getpos_start: ${x.getpos_start}, x.getpos_end: ${x.getpos_end}")


      if (x.getSymbol != sql.substring(x.getPosStart, x.getPosEnd)) {
        numErrors += 1
        println(s"ALERT: ERROR READING IN pos: $i line: ${x.getSymbolLine}; Valida:${if (x.getSymbol == sql.substring(x.getPosStart, x.getPosEnd)) "true" else "false" } , word: [${x.getSymbol}], largo: ${x.getSymbol.length()}, Pos Start: ${x.getPosStart}, Pos end: ${x.getPosEnd}, comprobar: [${sql.substring(x.getPosStart, x.getPosEnd)}] ")
      }

      i += 1
    }

    //Analyze SQL
    val res = analyzeSql(words, sql, tablesAndColumns)
    res.numErrors = numErrors
    res.autoIncSubQuery = numTempSubQuery
    numTempSubQuery += 1
    res.aliasQuery = s"TEMP_HUEMUL_$numTempSubQuery"
    res.aliasDatabase = "TEMP_HUEMUL"
    res
  }
}