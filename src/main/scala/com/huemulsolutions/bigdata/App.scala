package com.huemulsolutions.bigdata

import com.huemulsolutions.bigdata.sql_decode._
import scala.collection.mutable._

object App {

  def print_result(resfinal: HuemulSqlDecodeResult, numciclo: Int) {
    println(s"RESULTADO CICLO $numciclo ${resfinal.aliasQuery} ***************************************")
    println("************ SQL FROM ************ ")
    println(resfinal.fromSql)
    println("************ SQL WHERE ************ ")
    println(resfinal.whereSql)

    println("   ")
    println("************ COLUMNS ************ ")
    resfinal.columns.foreach { x =>
      println (s"*** COLUMN NAME: ${x.columnName}")
      println (s"    column_sql: ${x.columnSql}")
      println ("     columns used:")
      x.columnOrigin.foreach { y => println(s"     ---- column_database: ${y.traceDatabaseName}, trace_table_name: ${y.traceTableName}, trace_tableAlias_name: ${y.traceTableAliasName}, trace_column_name: ${y.traceColumnName}") }
    }

    println("   ")
    println("************ TABLES ************ ")
    resfinal.tables.foreach { x => println (s"*** DATABASE NAME: ${x.databaseName}, TABLE NAME: ${x.tableName}, ALIAS: ${x.tableAliasName}") }

    println("   ")
    println("************ COLUMNS WHERE ************ ")
    resfinal.columnsWhere.foreach { x => println(s"Columns: ${x.traceColumnName}, Table: ${x.traceTableName}, Database: ${x.traceDatabaseName}") }

    println("   ")
    println("************ FINAL RESULTS ************ ")
    println(s"Num Errores: ${resfinal.numErrors}")
    println(s"Num subquerys: ${resfinal.subQueryResult.length}")
    println(s"AliasDatabase: ${resfinal.aliasDatabase}")
    println(s"AliasQuery: ${resfinal.aliasQuery}")


    var numciclo_2 = numciclo
    resfinal.subQueryResult.foreach { x =>
      numciclo_2 += 1
      print_result(x,  numciclo_2)
    }

  }

  def main(args: Array[String]): Unit = {
    println("Version 1.0")

    val TabAndCols = new ArrayBuffer[HuemulSqlTablesAndColumns]

    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo3") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo1") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo5") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_no_existe", "campo11") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo7") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_no_existe", "campo10") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_2", "descripcion") )

    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_master", "Tablita2", "campo_mae_txt_2") )
    TabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_master", "Tablita2", "id_2") )


    val excludeWords: ArrayBuffer[String] = new ArrayBuffer[String]()
    val sql_dec: HuemulSqlDecode = new HuemulSqlDecode(excludeWords, 1)
    //sql_dec.decode("""SELECT descripcion + ")" + '()' + (10-20) FROM Tabla_2 tab where tab.campo1 = campo_1_orig""", TabAndCols)


    val resfinal = sql_dec.decodeSql("""SELECT CAMPO1,sum(campo1) as sumatoria, campo2 as rut, id
                            , campo3 nombre, campo4+campo5 *campo7 resultado, "nombre, de persona" texto_nuevo
             ,(SELECT descripcion + ")" + '()' + (10-20) FROM Tabla_2 tab where tab.campo10 = campo_1_orig) descripcion_avanzada
                            --, mae.campo_mae_txt
                            ,10 - -20 as campo_resta
                            , case when mae.campo_mae_txt = "hola" then 0
                                   when mae.campo_mae_txt = "chaito nomas" then 1
                                   else 3 end campo_resumen
                      FROM tabla_3 T3,
                           production_master.tbl_demo demo ,
                           production_master.tbl_tres ,
                           TablaOrigen orig
                         INNER JOIN (SELECT campo1, campo2 FROM Tabla3) otra
                             on otra.campo1 = tabla_3.campo2
                         INNER JOIN (select campo_mae_txt,id_2 as id FROM (select campo_mae_txt_2, id_2 from Tablita2) alias2) mae
                            on orig.campo_mae = mae.campo_mae
                         LEFT JOIN production_dim.tbl_left_join
                            on tbl_left_join.campo_a = mae.campo_a jull join tbl_full_join fjull on a = b
                      WHERE (orig.pk_1 = 10
--sinsalto
---otro sin saltito
                      AND pk_2 = 20)
                      or tabla_3.campo2 = "algo bueno" or campo7 = 30

                      """, TabAndCols)



    /*
   val resfinal = sql_dec.decodeSQL(
   """SELECT * from tabla_3""", TabAndCols)
   *
   */

    /*
     val resfinal = sql_dec.decodeSQL(
     """SELECT COUNT(1) AS players from tabla order by 1""", TabAndCols)
     */
    /*
    val resfinal = sql_dec.decodeSQL("""
    select a.col from tab1 a""", TabAndCols)
    */
    /*
    val resfinal = sql_dec.decodeSQL("""SELECT pet.name, comment FROM pet JOIN event ON (pet.name = event.name)""", TabAndCols)
    */
    /*
    val resfinal = sql_dec.decodeSQL("""
    SELECT teams.conference AS conference,
       players.school_name,
       COUNT(1) AS players
  FROM benn.college_football_players players
  JOIN benn.college_football_teams teams
    ON teams.school_name = players.school_name
 GROUP BY 1,2""", TabAndCols)
 *
 */
    /*
    val resfinal = sql_dec.decodeSQL("""SELECT teams.conference,
       sub.*
  FROM (
        SELECT players.school_name as yo,
               seba as otro_yo,
               players.school_name yo_sinas,
               seba otro_yo_sinas,
               COUNT(*) AS players,
COUNT(*) players_2
          FROM benn.college_football_players players
          where filtro = 2 and otro_filtro = sin_filtro
         GROUP BY 1
       ) sub
  JOIN benn.college_football_teams teams
  ON teams.school_name = sub.school_name
    """, TabAndCols)
    */

    print_result(resfinal, 1)

  }
}

//bug: 10 - -20 lo considera como comentario (desde el "- -20"), ver alguna forma de solucionar ese tipo de consultas
//