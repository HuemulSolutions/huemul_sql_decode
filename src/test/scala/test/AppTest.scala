package test

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.sql_decode._
import scala.collection.mutable._

@Test
class AppTest {
    val tabAndCols: ArrayBuffer[HuemulSqlTablesAndColumns] = new ArrayBuffer[HuemulSqlTablesAndColumns]()

      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo3") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo1") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo5") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_no_existe", "campo11") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_3", "campo7") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_no_existe", "campo10") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_dim", "tabla_2", "descripcion") )

      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_master", "Tablita2", "campo_mae_txt_2") )
      tabAndCols.append( new HuemulSqlTablesAndColumns().setData("prod_master", "Tablita2", "id_2") )


      var excludeWords:ArrayBuffer[String]  = new ArrayBuffer[String]()
      val sqlDec: HuemulSqlDecode = new HuemulSqlDecode(excludeWords, 1)


      val resFinalQuery1: HuemulSqlDecodeResult = sqlDec.decodeSql("""SELECT CAMPO1,sum(campo1) as sumatoria, campo2 as rut, id
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

                        """, tabAndCols)
                        
    
    @Test
    def testPlanQuery01T001Ok(): Unit = assertTrue(testPlanQuery01T001())
    
    @Test
    def testPlanQuery01T002Ok(): Unit = assertTrue(testPlanQuery01T002())
    
    @Test
    def testPlanQuery01T003Ok(): Unit = assertTrue(testPlanQuery01T003())
    
    @Test
    def testPlanQuery01T004Ok(): Unit = assertTrue(testPlanQuery01T004())
    
    @Test
    def testPlanQuery01T005_OK(): Unit = assertTrue(testPlanQuery01T005())
    
    @Test
    def testPlanQuery01T006_OK(): Unit = assertTrue(testPlanQuery01T006())
    
    @Test
    def testPlanQuery01T007_OK(): Unit = assertTrue(testPlanQuery01T007())
    
    @Test
    def testPlanQuery01T008_OK(): Unit = assertTrue(testPlanQuery01T008())
    
    @Test
    def testPlanQuery01T009_OK(): Unit = assertTrue(testPlanQuery01T009())
    
    @Test
    def testPlanQuery01C001_OK(): Unit = assertTrue(testPlanQuery01C001())
    @Test
    def testPlanQuery01C002_OK(): Unit = assertTrue(testPlanQuery01C002())
    @Test
    def testPlanQuery01C003_OK(): Unit = assertTrue(testPlanQuery01C003())
    @Test
    def testPlanQuery01C004_OK(): Unit = assertTrue(testPlanQuery01C004())
    @Test
    def testPlanQuery01C005_OK(): Unit = assertTrue(testPlanQuery01C005())
    @Test
    def testPlanQuery01C006_OK(): Unit = assertTrue(testPlanQuery01C006())
    @Test
    def testPlanQuery01C007_OK(): Unit = assertTrue(testPlanQuery01C007())
    @Test
    def testPlanQuery01C008_OK(): Unit = assertTrue(testPlanQuery01C008())
    @Test
    def testPlanQuery01C009_OK(): Unit = assertTrue(testPlanQuery01C009())
    @Test
    def testPlanQuery01C010_OK(): Unit = assertTrue(testPlanQuery01C010())
    
    @Test
    def testPlanQuery01Sq001T001_OK(): Unit = assertTrue(testPlanQuery01Sq001T001())
    @Test
    def testPlanQuery01SqResOK(): Unit = assertTrue(testPlanQuery01SqRes())
    @Test
    def testPlanQuery01Sq001T001C01_OK(): Unit = assertTrue(testPlanQuery01Sq001T001C01())
    @Test
    def testPlanQuery01Sq001T001C02_OK(): Unit = assertTrue(testPlanQuery01Sq001T001C02())

    /*********************************************************************************/
    /**************   V A L I D A C I O N   D E   C A M P O S    *********************/
    /*********************************************************************************/

    def testPlanQuery01C000(): Boolean = {
      var resultado_final: Boolean = true

      if (resFinalQuery1.columns.length != 10) {
        resultado_final = false
        println(s"error: cantidad de filas esperadas en columnas es 10, se obtienen ${resFinalQuery1.columns.length}")
      }
      resultado_final
    }

    def testPlanQuery01C001(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(0).columnName == "CAMPO1" &&
          resFinalQuery1.columns(0).columnSql == "CAMPO1" &&
          resFinalQuery1.columns(0).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(0) real/esperado column_name [${resFinalQuery1.columns(0).columnName}]/[CAMPO1], column_sql [${resFinalQuery1.columns(0).columnSql}]/[CAMPO1], column_origin [${resFinalQuery1.columns(0).columnOrigin.length}]/[1]")
      } else {
        if (!(resFinalQuery1.columns(0).columnOrigin(0).traceDatabaseName == "TEMP_HUEMUL" &&
              resFinalQuery1.columns(0).columnOrigin(0).traceTableName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(0).columnOrigin(0).traceTableAliasName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(0).columnOrigin(0).traceColumnName == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(0).columnOrigin(0).traceDatabaseName}]/[TEMP_HUEMUL],
                      trace_table_name [${resFinalQuery1.columns(0).columnOrigin(0).traceTableName}]/[TEMP_HUEMUL_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(0).columnOrigin(0).traceTableAliasName}]/[TEMP_HUEMUL_3],
                      trace_column_name [${resFinalQuery1.columns(0).columnOrigin(0).traceColumnName}]/[CAMPO1]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C002(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(1).columnName == "SUMATORIA" &&
          resFinalQuery1.columns(1).columnSql == "sum(campo1) as sumatoria" &&
          resFinalQuery1.columns(1).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(1) real/esperado column_name [${resFinalQuery1.columns(1).columnName}]/[SUMATORIA], column_sql [${resFinalQuery1.columns(1).columnSql}]/[sum(campo1) as sumatoria], column_origin [${resFinalQuery1.columns(1).columnOrigin.length}]/[1]")
      } else {
        if (!(resFinalQuery1.columns(1).columnOrigin(0).traceDatabaseName == "TEMP_HUEMUL" &&
              resFinalQuery1.columns(1).columnOrigin(0).traceTableName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(1).columnOrigin(0).traceTableAliasName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(1).columnOrigin(0).traceColumnName == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(1) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(1).columnOrigin(0).traceDatabaseName}]/[TEMP_HUEMUL],
                      trace_table_name [${resFinalQuery1.columns(1).columnOrigin(0).traceTableName}]/[TEMP_HUEMUL_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(1).columnOrigin(0).traceTableAliasName}]/[TEMP_HUEMUL_3],
                      trace_column_name [${resFinalQuery1.columns(1).columnOrigin(0).traceColumnName}]/[CAMPO1]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C003(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(2).columnName == "RUT" &&
          resFinalQuery1.columns(2).columnSql == "campo2 as rut" &&
          resFinalQuery1.columns(2).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(2) real/esperado column_name [${resFinalQuery1.columns(2).columnName}]/[RUT], column_sql [${resFinalQuery1.columns(2).columnSql}]/[campo2 as rut], column_origin [${resFinalQuery1.columns(2).columnOrigin.length}]/[1]")
      } else {
        if (!(resFinalQuery1.columns(2).columnOrigin(0).traceDatabaseName == "TEMP_HUEMUL" &&
              resFinalQuery1.columns(2).columnOrigin(0).traceTableName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(2).columnOrigin(0).traceTableAliasName == "TEMP_HUEMUL_3" &&
              resFinalQuery1.columns(2).columnOrigin(0).traceColumnName == "CAMPO2")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(2) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(2).columnOrigin(0).traceDatabaseName}]/[TEMP_HUEMUL],
                      trace_table_name [${resFinalQuery1.columns(2).columnOrigin(0).traceTableName}]/[TEMP_HUEMUL_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(2).columnOrigin(0).traceTableAliasName}]/[TEMP_HUEMUL_3],
                      trace_column_name [${resFinalQuery1.columns(2).columnOrigin(0).traceColumnName}]/[CAMPO2]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C004(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(3).columnName == "ID" &&
          resFinalQuery1.columns(3).columnSql == "id" &&
          resFinalQuery1.columns(3).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(3) real/esperado column_name [${resFinalQuery1.columns(3).columnName}]/[ID], column_sql [${resFinalQuery1.columns(3).columnSql}]/[id], column_origin [${resFinalQuery1.columns(3).columnOrigin.length}]/[1]")
      } else {
        if (!(resFinalQuery1.columns(3).columnOrigin(0).traceDatabaseName == "TEMP_HUEMUL" &&
              resFinalQuery1.columns(3).columnOrigin(0).traceTableName == "TEMP_HUEMUL_5" &&
              resFinalQuery1.columns(3).columnOrigin(0).traceTableAliasName == "TEMP_HUEMUL_5" &&
              resFinalQuery1.columns(3).columnOrigin(0).traceColumnName == "ID")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(3) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(3).columnOrigin(0).traceDatabaseName}]/[TEMP_HUEMUL],
                      trace_table_name [${resFinalQuery1.columns(3).columnOrigin(0).traceTableName}]/[TEMP_HUEMUL_5],
                      trace_tableAlias_name [${resFinalQuery1.columns(3).columnOrigin(0).traceTableAliasName}]/[TEMP_HUEMUL_5],
                      trace_column_name [${resFinalQuery1.columns(3).columnOrigin(0).traceColumnName}]/[ID]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C005(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(4).columnName == "NOMBRE" &&
          resFinalQuery1.columns(4).columnSql == "campo3 nombre" &&
          resFinalQuery1.columns(4).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(4) real/esperado column_name [${resFinalQuery1.columns(4).columnName}]/[NOMBRE], column_sql [${resFinalQuery1.columns(4).columnSql}]/[campo3 nombre], column_origin [${resFinalQuery1.columns(4).columnOrigin.length}]/[1]")
      } else {
        if (!(resFinalQuery1.columns(4).columnOrigin(0).traceDatabaseName == "prod_dim" &&
              resFinalQuery1.columns(4).columnOrigin(0).traceTableName == "tabla_3" &&
              resFinalQuery1.columns(4).columnOrigin(0).traceTableAliasName == "tabla_3" &&
              resFinalQuery1.columns(4).columnOrigin(0).traceColumnName == "CAMPO3")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(4) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(4).columnOrigin(0).traceDatabaseName}]/[prod_dim],
                      trace_table_name [${resFinalQuery1.columns(4).columnOrigin(0).traceTableName}]/[tabla_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(4).columnOrigin(0).traceTableAliasName}]/[tabla_3],
                      trace_column_name [${resFinalQuery1.columns(4).columnOrigin(0).traceColumnName}]/[CAMPO3]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C006(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(5).columnName == "RESULTADO" &&
          resFinalQuery1.columns(5).columnSql == "campo4+campo5 *campo7 resultado" &&
          resFinalQuery1.columns(5).columnOrigin.length == 3
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(5) real/esperado column_name [${resFinalQuery1.columns(5).columnName}]/[RESULTADO], column_sql [${resFinalQuery1.columns(5).columnSql}]/[campo4+campo5 *campo7 resultado], column_origin [${resFinalQuery1.columns(5).columnOrigin.length}]/[3]")
      } else {
        if (!(resFinalQuery1.columns(5).columnOrigin(0).traceDatabaseName == null &&
              resFinalQuery1.columns(5).columnOrigin(0).traceTableName == null &&
              resFinalQuery1.columns(5).columnOrigin(0).traceTableAliasName == null &&
              resFinalQuery1.columns(5).columnOrigin(0).traceColumnName == "CAMPO4")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(5).columnOrigin(0).traceDatabaseName}]/[null],
                      trace_table_name [${resFinalQuery1.columns(5).columnOrigin(0).traceTableName}]/[null],
                      trace_tableAlias_name [${resFinalQuery1.columns(5).columnOrigin(0).traceTableAliasName}]/[null],
                      trace_column_name [${resFinalQuery1.columns(5).columnOrigin(0).traceColumnName}]/[CAMPO4]  """)
        }

        if (!(resFinalQuery1.columns(5).columnOrigin(1).traceDatabaseName == "prod_dim" &&
              resFinalQuery1.columns(5).columnOrigin(1).traceTableName == "tabla_3" &&
              resFinalQuery1.columns(5).columnOrigin(1).traceTableAliasName == "tabla_3" &&
              resFinalQuery1.columns(5).columnOrigin(1).traceColumnName == "CAMPO5")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(1) real/esperado
                      trace_database_name [${resFinalQuery1.columns(5).columnOrigin(1).traceDatabaseName}]/[prod_dim],
                      trace_table_name [${resFinalQuery1.columns(5).columnOrigin(1).traceTableName}]/[tabla_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(5).columnOrigin(1).traceTableAliasName}]/[tabla_3],
                      trace_column_name [${resFinalQuery1.columns(5).columnOrigin(1).traceColumnName}]/[CAMPO5]  """)
        }

        if (!(resFinalQuery1.columns(5).columnOrigin(2).traceDatabaseName == "prod_dim" &&
              resFinalQuery1.columns(5).columnOrigin(2).traceTableName == "tabla_3" &&
              resFinalQuery1.columns(5).columnOrigin(2).traceTableAliasName == "tabla_3" &&
              resFinalQuery1.columns(5).columnOrigin(2).traceColumnName == "CAMPO7")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(2) real/esperado
                      trace_database_name [${resFinalQuery1.columns(5).columnOrigin(2).traceDatabaseName}]/[prod_dim],
                      trace_table_name [${resFinalQuery1.columns(5).columnOrigin(2).traceTableName}]/[tabla_3],
                      trace_tableAlias_name [${resFinalQuery1.columns(5).columnOrigin(2).traceTableAliasName}]/[tabla_3],
                      trace_column_name [${resFinalQuery1.columns(5).columnOrigin(2).traceColumnName}]/[CAMPO7]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C007(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(6).columnName == "TEXTO_NUEVO" &&
          resFinalQuery1.columns(6).columnSql == """"nombre, de persona" texto_nuevo""" &&
          resFinalQuery1.columns(6).columnOrigin.isEmpty
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(6) real/esperado column_name [${resFinalQuery1.columns(6).columnName}]/[TEXTO_NUEVO], column_sql [${resFinalQuery1.columns(6).columnSql}]/["nombre, de persona" texto_nuevo], column_origin [${resFinalQuery1.columns(6).columnOrigin.length}]/[0]""")
      }
      resultado_final
    }

    def testPlanQuery01C008(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(7).columnName == "DESCRIPCION_AVANZADA" &&
          resFinalQuery1.columns(7).columnSql == """(SELECT descripcion + ")" + '()' + (10-20) FROM Tabla_2 tab where tab.campo10 = campo_1_orig) descripcion_avanzada""" &&
          resFinalQuery1.columns(7).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(7) real/esperado column_name [${resFinalQuery1.columns(7).columnName}]/[DESCRIPCION_AVANZADA], column_sql [${resFinalQuery1.columns(7).columnSql}]/[campo3 nombre], column_origin [${resFinalQuery1.columns(7).columnOrigin.length}]/[1]""")
      } else {
        if (!(resFinalQuery1.columns(7).columnOrigin(0).traceDatabaseName == "prod_dim" &&
              resFinalQuery1.columns(7).columnOrigin(0).traceTableName == "tabla_2" &&
              resFinalQuery1.columns(7).columnOrigin(0).traceTableAliasName == "tabla_2" &&
              resFinalQuery1.columns(7).columnOrigin(0).traceColumnName == "DESCRIPCION")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(7) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(7).columnOrigin(0).traceDatabaseName}]/[prod_dim],
                      trace_table_name [${resFinalQuery1.columns(7).columnOrigin(0).traceTableName}]/[tabla_2],
                      trace_tableAlias_name [${resFinalQuery1.columns(7).columnOrigin(0).traceTableAliasName}]/[tabla_2],
                      trace_column_name [${resFinalQuery1.columns(7).columnOrigin(0).traceColumnName}]/[DESCRIPCION]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01C009(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.columns(8).columnName == "10" &&
          resFinalQuery1.columns(8).columnSql == """10""" &&
          resFinalQuery1.columns(8).columnOrigin.isEmpty
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(8) real/esperado column_name [${resFinalQuery1.columns(8).columnName}]/[10], column_sql [${resFinalQuery1.columns(8).columnSql}]/[10], column_origin [${resFinalQuery1.columns(8).columnOrigin.length}]/[0]""")
      }
      resultado_final
    }

    def testPlanQuery01C010(): Boolean = {
      var resultado_final: Boolean = true
      if (!(resFinalQuery1.columns(9).columnName == "CAMPO_RESUMEN" &&
          resFinalQuery1.columns(9).columnSql.hashCode() == """case when mae.campo_mae_txt = "hola" then 0
                                     when mae.campo_mae_txt = "chaito nomas" then 1
                                     else 3 end campo_resumen""".hashCode() &&
          resFinalQuery1.columns(9).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(9) real/esperado column_name [${resFinalQuery1.columns(9).columnName}]/[CAMPO_RESUMEN], column_sql [${resFinalQuery1.columns(9).columnSql}]/[case when mae.campo_mae_txt = "hola" then 0
                                   when mae.campo_mae_txt = "chaito nomas" then 1
                                   else 3 end campo_resumen], column_origin [${resFinalQuery1.columns(9).columnOrigin.length}]/[1]""")
      } else {
        if (!(resFinalQuery1.columns(9).columnOrigin(0).traceDatabaseName == "TEMP_HUEMUL" &&
              resFinalQuery1.columns(9).columnOrigin(0).traceTableName == "TEMP_HUEMUL_5" &&
              resFinalQuery1.columns(9).columnOrigin(0).traceTableAliasName == "MAE" &&
              resFinalQuery1.columns(9).columnOrigin(0).traceColumnName == "CAMPO_MAE_TXT")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(9) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.columns(9).columnOrigin(0).traceDatabaseName}]/[TEMP_HUEMUL],
                      trace_table_name [${resFinalQuery1.columns(9).columnOrigin(0).traceTableName}]/[TEMP_HUEMUL_5],
                      trace_tableAlias_name [${resFinalQuery1.columns(9).columnOrigin(0).traceTableAliasName}]/[MAE],
                      trace_column_name [${resFinalQuery1.columns(9).columnOrigin(0).traceColumnName}]/[CAMPO_MAE_TXT]  """)
        }
      }
      resultado_final
    }

    /*********************************************************************************/
    /**************   V A L I D A C I O N   D E   T A B L A S    *********************/
    /*********************************************************************************/
    def testPlanQuery01T001(): Boolean = {
      var resultado_final: Boolean = true

      if (resFinalQuery1.tables.length != 8) {
        resultado_final = false
        println(s"error: cantidad de filas esperadas es 8, se obtienen ${resFinalQuery1.tables.length}")
      }
      resultado_final
    }

    def testPlanQuery01T002(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(0).databaseName == "prod_dim" &&
          resFinalQuery1.tables(0).tableName == "TABLA_3" &&
          resFinalQuery1.tables(0).tableAliasName == "T3")) {
        resultado_final = false
        println(s"error: tabla(0) real/esperado database_name [${resFinalQuery1.tables(0).databaseName}]/[prod_dim], table_name [${resFinalQuery1.tables(0).tableName}]/[TABLA_3], database_name [${resFinalQuery1.tables(0).tableAliasName}]/[T3]")
      }
      resultado_final
    }

    def testPlanQuery01T003(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(1).databaseName == "PRODUCTION_MASTER" &&
          resFinalQuery1.tables(1).tableName == "TBL_DEMO" &&
          resFinalQuery1.tables(1).tableAliasName == "DEMO")) {
        resultado_final = false
        println(s"error: tabla(1) real/esperado database_name [${resFinalQuery1.tables(1).databaseName}]/[PRODUCTION_MASTER], table_name [${resFinalQuery1.tables(1).tableName}]/[TBL_DEMO], database_name [${resFinalQuery1.tables(1).tableAliasName}]/[DEMO]")
      }
      resultado_final
    }

    def testPlanQuery01T004(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(2).databaseName == "PRODUCTION_MASTER" &&
          resFinalQuery1.tables(2).tableName == "TBL_TRES" &&
          resFinalQuery1.tables(2).tableAliasName == "TBL_TRES")) {
        resultado_final = false
        println(s"error: tabla(2) real/esperado database_name [${resFinalQuery1.tables(2).databaseName}]/[PRODUCTION_MASTER], table_name [${resFinalQuery1.tables(2).tableName}]/[TBL_TRES], database_name [${resFinalQuery1.tables(2).tableAliasName}]/[TBL_TRES]")
      }
      resultado_final
    }

    def testPlanQuery01T005(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(3).databaseName == null &&
          resFinalQuery1.tables(3).tableName == "TABLAORIGEN" &&
          resFinalQuery1.tables(3).tableAliasName == "ORIG")) {
        resultado_final = false
        println(s"error: tabla(3) real/esperado database_name [${resFinalQuery1.tables(3).databaseName}]/[null], table_name [${resFinalQuery1.tables(3).tableName}]/[TABLAORIGEN], database_name [${resFinalQuery1.tables(3).tableAliasName}]/[ORIG]")
      }
      resultado_final
    }

    def testPlanQuery01T006(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(4).databaseName == "TEMP_HUEMUL" &&
          resFinalQuery1.tables(4).tableName == "TEMP_HUEMUL_3" &&
          resFinalQuery1.tables(4).tableAliasName == "OTRA")) {
        resultado_final = false
        println(s"error: tabla(4) real/esperado database_name [${resFinalQuery1.tables(4).databaseName}]/[TEMP_HUEMUL], table_name [${resFinalQuery1.tables(4).tableName}]/[TEMP_HUEMUL_3], database_name [${resFinalQuery1.tables(4).tableAliasName}]/[OTRA]")
      }
      resultado_final
    }

    def testPlanQuery01T007(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(5).databaseName == "TEMP_HUEMUL" &&
          resFinalQuery1.tables(5).tableName == "TEMP_HUEMUL_5" &&
          resFinalQuery1.tables(5).tableAliasName == "MAE")) {
        resultado_final = false
        println(s"error: tabla(5) real/esperado database_name [${resFinalQuery1.tables(5).databaseName}]/[TEMP_HUEMUL], table_name [${resFinalQuery1.tables(5).tableName}]/[TEMP_HUEMUL_5], database_name [${resFinalQuery1.tables(5).tableAliasName}]/[MAE]")
      }
      resultado_final
    }

    def testPlanQuery01T008(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(6).databaseName == "PRODUCTION_DIM" &&
          resFinalQuery1.tables(6).tableName == "TBL_LEFT_JOIN" &&
          resFinalQuery1.tables(6).tableAliasName == "TBL_LEFT_JOIN")) {
        resultado_final = false
        println(s"error: tabla(6) real/esperado database_name [${resFinalQuery1.tables(6).databaseName}]/[PRODUCTION_DIM], table_name [${resFinalQuery1.tables(6).tableName}]/[TBL_LEFT_JOIN], database_name [${resFinalQuery1.tables(6).tableAliasName}]/[TBL_LEFT_JOIN]")
      }
      resultado_final
    }

    def testPlanQuery01T009(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.tables(7).databaseName == null &&
          resFinalQuery1.tables(7).tableName == "TBL_FULL_JOIN" &&
          resFinalQuery1.tables(7).tableAliasName == "FJULL")) {
        resultado_final = false
        println(s"error: tabla(7) real/esperado database_name [${resFinalQuery1.tables(7).databaseName}]/[null], table_name [${resFinalQuery1.tables(7).tableName}]/[TBL_FULL_JOIN], database_name [${resFinalQuery1.tables(7).tableAliasName}]/[FJULL]")
      }
      resultado_final
    }




    def testPlanQuery01SqRes(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.subQueryResult.length == 2)){
        resultado_final = false
        println(s"error: subquery, esperado 2, real ${resFinalQuery1.subQueryResult.length} ")
      }
      resultado_final
    }

    def testPlanQuery01Sq001T001(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.subQueryResult(0).tables(0).databaseName == null &&
          resFinalQuery1.subQueryResult(0).tables(0).tableName == "TABLA3" &&
          resFinalQuery1.subQueryResult(0).tables(0).tableAliasName == "TABLA3")) {
        resultado_final = false
        println(s"error: subquery(0) tabla(0) real/esperado database_name [${resFinalQuery1.subQueryResult(0).tables(0).databaseName}]/[null], table_name [${resFinalQuery1.subQueryResult(0).tables(0).tableName}]/[TABLA3], database_name [${resFinalQuery1.subQueryResult(0).tables(0).tableAliasName}]/[TABLA3]")
      }
      resultado_final
    }

    def testPlanQuery01Sq001T001C01(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.subQueryResult(0).columns(0).columnName == "CAMPO1" &&
          resFinalQuery1.subQueryResult(0).columns(0).columnSql == """campo1""" &&
          resFinalQuery1.subQueryResult(0).columns(0).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(0) real/esperado column_name [${resFinalQuery1.subQueryResult(0).columns(0).columnName}]/[CAMPO1], column_sql [${resFinalQuery1.subQueryResult(0).columns(0).columnSql}]/[campo1], column_origin [${resFinalQuery1.subQueryResult(0).columns(0).columnOrigin.length}]/[1]""")
      } else {
        if (!(resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceDatabaseName == null &&
              resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceTableName == null &&
              resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceTableAliasName == null &&
              resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceColumnName == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceDatabaseName}]/[null],
                      trace_table_name [${resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceTableName}]/[null],
                      trace_tableAlias_name [${resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceTableAliasName}]/[null],
                      trace_column_name [${resFinalQuery1.subQueryResult(0).columns(0).columnOrigin(0).traceColumnName}]/[CAMPO1]  """)
        }
      }
      resultado_final
    }

    def testPlanQuery01Sq001T001C02(): Boolean = {
      var resultado_final: Boolean = true

      if (!(resFinalQuery1.subQueryResult(0).columns(1).columnName == "CAMPO2" &&
          resFinalQuery1.subQueryResult(0).columns(1).columnSql == """campo2""" &&
          resFinalQuery1.subQueryResult(0).columns(1).columnOrigin.length == 1
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(0) real/esperado column_name [${resFinalQuery1.subQueryResult(0).columns(1).columnName}]/[CAMPO2], column_sql [${resFinalQuery1.subQueryResult(0).columns(1).columnSql}]/[campo2], column_origin [${resFinalQuery1.subQueryResult(0).columns(1).columnOrigin.length}]/[1]""")
      } else {
        if (!(resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceDatabaseName == null &&
              resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceTableName == null &&
              resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceTableAliasName == null &&
              resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceColumnName == "CAMPO2")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado
                      trace_database_name [${resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceDatabaseName}]/[null],
                      trace_table_name [${resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceTableName}]/[null],
                      trace_tableAlias_name [${resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceTableAliasName}]/[null],
                      trace_column_name [${resFinalQuery1.subQueryResult(0).columns(1).columnOrigin(0).traceColumnName}]/[CAMPO2]  """)
        }
      }
      resultado_final
    }

}
