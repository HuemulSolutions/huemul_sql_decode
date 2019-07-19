package test

import org.junit._
import Assert._
import com.huemulsolutions.bigdata.sql_decode._
import scala.collection.mutable._

@Test
class AppTest {
    val TabAndCols: ArrayBuffer[huemul_sql_tables_and_columns] = new ArrayBuffer[huemul_sql_tables_and_columns]()
     
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_3", "campo3")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_3", "campo1")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_3", "campo5")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_no_existe", "campo11")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_3", "campo7")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_no_existe", "campo10")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_dim", "tabla_2", "descripcion")) )
      
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_master", "Tablita2", "campo_mae_txt_2")) )
      TabAndCols.append( (new huemul_sql_tables_and_columns().setData("prod_master", "Tablita2", "id_2")) )
      
      
      var excludeWords:ArrayBuffer[String]  = new ArrayBuffer[String]() 
      val sql_dec: huemul_SQL_Decode = new huemul_SQL_Decode(excludeWords, 1)
      
      
      val resfinal_query1 = sql_dec.decodeSQL("""SELECT CAMPO1,sum(campo1) as sumatoria, campo2 as rut, id
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
                        
    
    @Test
    def testplan_query01_T001_OK() = assertTrue(testplan_query01_T001)
    
    @Test
    def testplan_query01_T002_OK() = assertTrue(testplan_query01_T002)
    
    @Test
    def testplan_query01_T003_OK() = assertTrue(testplan_query01_T003)
    
    @Test
    def testplan_query01_T004_OK() = assertTrue(testplan_query01_T004)
    
    @Test
    def testplan_query01_T005_OK() = assertTrue(testplan_query01_T005)
    
    @Test
    def testplan_query01_T006_OK() = assertTrue(testplan_query01_T006)
    
    @Test
    def testplan_query01_T007_OK() = assertTrue(testplan_query01_T007)
    
    @Test
    def testplan_query01_T008_OK() = assertTrue(testplan_query01_T008)
    
    @Test
    def testplan_query01_T009_OK() = assertTrue(testplan_query01_T009)
    
    @Test
    def testplan_query01_C001_OK() = assertTrue(testplan_query01_C001)
    @Test
    def testplan_query01_C002_OK() = assertTrue(testplan_query01_C002)
    @Test
    def testplan_query01_C003_OK() = assertTrue(testplan_query01_C003)
    @Test
    def testplan_query01_C004_OK() = assertTrue(testplan_query01_C004)
    @Test
    def testplan_query01_C005_OK() = assertTrue(testplan_query01_C005)
    @Test
    def testplan_query01_C006_OK() = assertTrue(testplan_query01_C006)
    @Test
    def testplan_query01_C007_OK() = assertTrue(testplan_query01_C007)
    @Test
    def testplan_query01_C008_OK() = assertTrue(testplan_query01_C008)
    @Test
    def testplan_query01_C009_OK() = assertTrue(testplan_query01_C009)
    @Test
    def testplan_query01_C010_OK() = assertTrue(testplan_query01_C010)
    
    @Test
    def testplan_query01_SQ_001_T001_OK()= assertTrue(testplan_query01_SQ_001_T001)
    @Test
    def testplan_query01_SQRes_OK()= assertTrue(testplan_query01_SQRes)
    @Test
    def testplan_query01_SQ_001_T001_C01_OK() = assertTrue(testplan_query01_SQ_001_T001_C01)
    @Test
    def testplan_query01_SQ_001_T001_C02_OK() = assertTrue(testplan_query01_SQ_001_T001_C02)
    
    /*********************************************************************************/
    /**************   V A L I D A C I O N   D E   C A M P O S    *********************/
    /*********************************************************************************/
    
    def testplan_query01_C000(): Boolean = {
      var resultado_final: Boolean = true
      
      if (resfinal_query1.columns.length != 10) {
        resultado_final = false
        println(s"error: cantidad de filas esperadas en columnas es 10, se obtienen ${resfinal_query1.columns.length}")
      }
      return resultado_final
    }
    
    def testplan_query01_C001(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(0).column_name == "CAMPO1" && 
          resfinal_query1.columns(0).column_sql == "CAMPO1" &&
          resfinal_query1.columns(0).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(0) real/esperado column_name [${resfinal_query1.columns(0).column_name}]/[CAMPO1], column_sql [${resfinal_query1.columns(0).column_sql}]/[CAMPO1], column_origin [${resfinal_query1.columns(0).column_origin.length}]/[1]")
      } else {
        if (!(resfinal_query1.columns(0).column_origin(0).trace_database_name == "TEMP_HUEMUL" &&
              resfinal_query1.columns(0).column_origin(0).trace_table_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(0).column_origin(0).trace_tableAlias_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(0).column_origin(0).trace_column_name == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(0).column_origin(0).trace_database_name}]/[TEMP_HUEMUL], 
                      trace_table_name [${resfinal_query1.columns(0).column_origin(0).trace_table_name}]/[TEMP_HUEMUL_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(0).column_origin(0).trace_tableAlias_name}]/[TEMP_HUEMUL_3], 
                      trace_column_name [${resfinal_query1.columns(0).column_origin(0).trace_column_name}]/[CAMPO1]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C002(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(1).column_name == "SUMATORIA" && 
          resfinal_query1.columns(1).column_sql == "sum(campo1) as sumatoria" &&
          resfinal_query1.columns(1).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(1) real/esperado column_name [${resfinal_query1.columns(1).column_name}]/[SUMATORIA], column_sql [${resfinal_query1.columns(1).column_sql}]/[sum(campo1) as sumatoria], column_origin [${resfinal_query1.columns(1).column_origin.length}]/[1]")
      } else {
        if (!(resfinal_query1.columns(1).column_origin(0).trace_database_name == "TEMP_HUEMUL" &&
              resfinal_query1.columns(1).column_origin(0).trace_table_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(1).column_origin(0).trace_tableAlias_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(1).column_origin(0).trace_column_name == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(1) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(1).column_origin(0).trace_database_name}]/[TEMP_HUEMUL], 
                      trace_table_name [${resfinal_query1.columns(1).column_origin(0).trace_table_name}]/[TEMP_HUEMUL_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(1).column_origin(0).trace_tableAlias_name}]/[TEMP_HUEMUL_3], 
                      trace_column_name [${resfinal_query1.columns(1).column_origin(0).trace_column_name}]/[CAMPO1]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C003(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(2).column_name == "RUT" && 
          resfinal_query1.columns(2).column_sql == "campo2 as rut" &&
          resfinal_query1.columns(2).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(2) real/esperado column_name [${resfinal_query1.columns(2).column_name}]/[RUT], column_sql [${resfinal_query1.columns(2).column_sql}]/[campo2 as rut], column_origin [${resfinal_query1.columns(2).column_origin.length}]/[1]")
      } else {
        if (!(resfinal_query1.columns(2).column_origin(0).trace_database_name == "TEMP_HUEMUL" &&
              resfinal_query1.columns(2).column_origin(0).trace_table_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(2).column_origin(0).trace_tableAlias_name == "TEMP_HUEMUL_3" &&
              resfinal_query1.columns(2).column_origin(0).trace_column_name == "CAMPO2")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(2) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(2).column_origin(0).trace_database_name}]/[TEMP_HUEMUL], 
                      trace_table_name [${resfinal_query1.columns(2).column_origin(0).trace_table_name}]/[TEMP_HUEMUL_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(2).column_origin(0).trace_tableAlias_name}]/[TEMP_HUEMUL_3], 
                      trace_column_name [${resfinal_query1.columns(2).column_origin(0).trace_column_name}]/[CAMPO2]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C004(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(3).column_name == "ID" && 
          resfinal_query1.columns(3).column_sql == "id" &&
          resfinal_query1.columns(3).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(3) real/esperado column_name [${resfinal_query1.columns(3).column_name}]/[ID], column_sql [${resfinal_query1.columns(3).column_sql}]/[id], column_origin [${resfinal_query1.columns(3).column_origin.length}]/[1]")
      } else {
        if (!(resfinal_query1.columns(3).column_origin(0).trace_database_name == "TEMP_HUEMUL" &&
              resfinal_query1.columns(3).column_origin(0).trace_table_name == "TEMP_HUEMUL_5" &&
              resfinal_query1.columns(3).column_origin(0).trace_tableAlias_name == "TEMP_HUEMUL_5" &&
              resfinal_query1.columns(3).column_origin(0).trace_column_name == "ID")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(3) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(3).column_origin(0).trace_database_name}]/[TEMP_HUEMUL], 
                      trace_table_name [${resfinal_query1.columns(3).column_origin(0).trace_table_name}]/[TEMP_HUEMUL_5], 
                      trace_tableAlias_name [${resfinal_query1.columns(3).column_origin(0).trace_tableAlias_name}]/[TEMP_HUEMUL_5], 
                      trace_column_name [${resfinal_query1.columns(3).column_origin(0).trace_column_name}]/[ID]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C005(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(4).column_name == "NOMBRE" && 
          resfinal_query1.columns(4).column_sql == "campo3 nombre" &&
          resfinal_query1.columns(4).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(4) real/esperado column_name [${resfinal_query1.columns(4).column_name}]/[NOMBRE], column_sql [${resfinal_query1.columns(4).column_sql}]/[campo3 nombre], column_origin [${resfinal_query1.columns(4).column_origin.length}]/[1]")
      } else {
        if (!(resfinal_query1.columns(4).column_origin(0).trace_database_name == "prod_dim" &&
              resfinal_query1.columns(4).column_origin(0).trace_table_name == "tabla_3" &&
              resfinal_query1.columns(4).column_origin(0).trace_tableAlias_name == "tabla_3" &&
              resfinal_query1.columns(4).column_origin(0).trace_column_name == "CAMPO3")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(4) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(4).column_origin(0).trace_database_name}]/[prod_dim], 
                      trace_table_name [${resfinal_query1.columns(4).column_origin(0).trace_table_name}]/[tabla_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(4).column_origin(0).trace_tableAlias_name}]/[tabla_3], 
                      trace_column_name [${resfinal_query1.columns(4).column_origin(0).trace_column_name}]/[CAMPO3]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C006(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(5).column_name == "RESULTADO" && 
          resfinal_query1.columns(5).column_sql == "campo4+campo5 *campo7 resultado" &&
          resfinal_query1.columns(5).column_origin.length == 3 
          )) {
        resultado_final = false
        println(s"error: tabla(0) campo(5) real/esperado column_name [${resfinal_query1.columns(5).column_name}]/[RESULTADO], column_sql [${resfinal_query1.columns(5).column_sql}]/[campo4+campo5 *campo7 resultado], column_origin [${resfinal_query1.columns(5).column_origin.length}]/[3]")
      } else {
        if (!(resfinal_query1.columns(5).column_origin(0).trace_database_name == null &&
              resfinal_query1.columns(5).column_origin(0).trace_table_name == null &&
              resfinal_query1.columns(5).column_origin(0).trace_tableAlias_name == null &&
              resfinal_query1.columns(5).column_origin(0).trace_column_name == "CAMPO4")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(5).column_origin(0).trace_database_name}]/[null], 
                      trace_table_name [${resfinal_query1.columns(5).column_origin(0).trace_table_name}]/[null], 
                      trace_tableAlias_name [${resfinal_query1.columns(5).column_origin(0).trace_tableAlias_name}]/[null], 
                      trace_column_name [${resfinal_query1.columns(5).column_origin(0).trace_column_name}]/[CAMPO4]  """)        
        }
        
        if (!(resfinal_query1.columns(5).column_origin(1).trace_database_name == "prod_dim" &&
              resfinal_query1.columns(5).column_origin(1).trace_table_name == "tabla_3" &&
              resfinal_query1.columns(5).column_origin(1).trace_tableAlias_name == "tabla_3" &&
              resfinal_query1.columns(5).column_origin(1).trace_column_name == "CAMPO5")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(1) real/esperado 
                      trace_database_name [${resfinal_query1.columns(5).column_origin(1).trace_database_name}]/[prod_dim], 
                      trace_table_name [${resfinal_query1.columns(5).column_origin(1).trace_table_name}]/[tabla_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(5).column_origin(1).trace_tableAlias_name}]/[tabla_3], 
                      trace_column_name [${resfinal_query1.columns(5).column_origin(1).trace_column_name}]/[CAMPO5]  """)        
        }
        
        if (!(resfinal_query1.columns(5).column_origin(2).trace_database_name == "prod_dim" &&
              resfinal_query1.columns(5).column_origin(2).trace_table_name == "tabla_3" &&
              resfinal_query1.columns(5).column_origin(2).trace_tableAlias_name == "tabla_3" &&
              resfinal_query1.columns(5).column_origin(2).trace_column_name == "CAMPO7")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(5) origin(2) real/esperado 
                      trace_database_name [${resfinal_query1.columns(5).column_origin(2).trace_database_name}]/[prod_dim], 
                      trace_table_name [${resfinal_query1.columns(5).column_origin(2).trace_table_name}]/[tabla_3], 
                      trace_tableAlias_name [${resfinal_query1.columns(5).column_origin(2).trace_tableAlias_name}]/[tabla_3], 
                      trace_column_name [${resfinal_query1.columns(5).column_origin(2).trace_column_name}]/[CAMPO7]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C007(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(6).column_name == "TEXTO_NUEVO" && 
          resfinal_query1.columns(6).column_sql == """"nombre, de persona" texto_nuevo""" &&
          resfinal_query1.columns(6).column_origin.length == 0 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(6) real/esperado column_name [${resfinal_query1.columns(6).column_name}]/[TEXTO_NUEVO], column_sql [${resfinal_query1.columns(6).column_sql}]/["nombre, de persona" texto_nuevo], column_origin [${resfinal_query1.columns(6).column_origin.length}]/[0]""")
      } 
      return resultado_final
    }
    
    def testplan_query01_C008(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(7).column_name == "DESCRIPCION_AVANZADA" && 
          resfinal_query1.columns(7).column_sql == """(SELECT descripcion + ")" + '()' + (10-20) FROM Tabla_2 tab where tab.campo10 = campo_1_orig) descripcion_avanzada""" &&
          resfinal_query1.columns(7).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(7) real/esperado column_name [${resfinal_query1.columns(7).column_name}]/[DESCRIPCION_AVANZADA], column_sql [${resfinal_query1.columns(7).column_sql}]/[campo3 nombre], column_origin [${resfinal_query1.columns(7).column_origin.length}]/[1]""")
      } else {
        if (!(resfinal_query1.columns(7).column_origin(0).trace_database_name == "prod_dim" &&
              resfinal_query1.columns(7).column_origin(0).trace_table_name == "tabla_2" &&
              resfinal_query1.columns(7).column_origin(0).trace_tableAlias_name == "tabla_2" &&
              resfinal_query1.columns(7).column_origin(0).trace_column_name == "DESCRIPCION")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(7) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(7).column_origin(0).trace_database_name}]/[prod_dim], 
                      trace_table_name [${resfinal_query1.columns(7).column_origin(0).trace_table_name}]/[tabla_2], 
                      trace_tableAlias_name [${resfinal_query1.columns(7).column_origin(0).trace_tableAlias_name}]/[tabla_2], 
                      trace_column_name [${resfinal_query1.columns(7).column_origin(0).trace_column_name}]/[DESCRIPCION]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_C009(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.columns(8).column_name == "10" && 
          resfinal_query1.columns(8).column_sql == """10""" &&
          resfinal_query1.columns(8).column_origin.length == 0 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(8) real/esperado column_name [${resfinal_query1.columns(8).column_name}]/[10], column_sql [${resfinal_query1.columns(8).column_sql}]/[10], column_origin [${resfinal_query1.columns(8).column_origin.length}]/[0]""")
      } 
      return resultado_final
    }
    
    def testplan_query01_C010(): Boolean = {
      var resultado_final: Boolean = true
      if (!(resfinal_query1.columns(9).column_name == "CAMPO_RESUMEN" && 
          resfinal_query1.columns(9).column_sql.hashCode() == """case when mae.campo_mae_txt = "hola" then 0
                                     when mae.campo_mae_txt = "chaito nomas" then 1
                                     else 3 end campo_resumen""".hashCode() &&
          resfinal_query1.columns(9).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(9) real/esperado column_name [${resfinal_query1.columns(9).column_name}]/[CAMPO_RESUMEN], column_sql [${resfinal_query1.columns(9).column_sql}]/[case when mae.campo_mae_txt = "hola" then 0
                                   when mae.campo_mae_txt = "chaito nomas" then 1
                                   else 3 end campo_resumen], column_origin [${resfinal_query1.columns(9).column_origin.length}]/[1]""")
      } else {
        if (!(resfinal_query1.columns(9).column_origin(0).trace_database_name == "TEMP_HUEMUL" &&
              resfinal_query1.columns(9).column_origin(0).trace_table_name == "TEMP_HUEMUL_5" &&
              resfinal_query1.columns(9).column_origin(0).trace_tableAlias_name == "MAE" &&
              resfinal_query1.columns(9).column_origin(0).trace_column_name == "CAMPO_MAE_TXT")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(9) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.columns(9).column_origin(0).trace_database_name}]/[TEMP_HUEMUL], 
                      trace_table_name [${resfinal_query1.columns(9).column_origin(0).trace_table_name}]/[TEMP_HUEMUL_5], 
                      trace_tableAlias_name [${resfinal_query1.columns(9).column_origin(0).trace_tableAlias_name}]/[MAE], 
                      trace_column_name [${resfinal_query1.columns(9).column_origin(0).trace_column_name}]/[CAMPO_MAE_TXT]  """)        
        }
      }
      return resultado_final
    }
    
    /*********************************************************************************/
    /**************   V A L I D A C I O N   D E   T A B L A S    *********************/
    /*********************************************************************************/
    def testplan_query01_T001(): Boolean = {
      var resultado_final: Boolean = true
      
      if (resfinal_query1.tables.length != 8) {
        resultado_final = false
        println(s"error: cantidad de filas esperadas es 8, se obtienen ${resfinal_query1.tables.length}")
      }
      return resultado_final
    }
    
    def testplan_query01_T002(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(0).database_name == "prod_dim" && 
          resfinal_query1.tables(0).table_name == "TABLA_3" &&
          resfinal_query1.tables(0).tableAlias_name == "T3")) {
        resultado_final = false
        println(s"error: tabla(0) real/esperado database_name [${resfinal_query1.tables(0).database_name}]/[prod_dim], table_name [${resfinal_query1.tables(0).table_name}]/[TABLA_3], database_name [${resfinal_query1.tables(0).tableAlias_name}]/[T3]")
      }
      return resultado_final
    }
    
    def testplan_query01_T003(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(1).database_name == "PRODUCTION_MASTER" && 
          resfinal_query1.tables(1).table_name == "TBL_DEMO" &&
          resfinal_query1.tables(1).tableAlias_name == "DEMO")) {
        resultado_final = false
        println(s"error: tabla(1) real/esperado database_name [${resfinal_query1.tables(1).database_name}]/[PRODUCTION_MASTER], table_name [${resfinal_query1.tables(1).table_name}]/[TBL_DEMO], database_name [${resfinal_query1.tables(1).tableAlias_name}]/[DEMO]")
      }
      return resultado_final
    }
      
    def testplan_query01_T004(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(2).database_name == "PRODUCTION_MASTER" && 
          resfinal_query1.tables(2).table_name == "TBL_TRES" &&
          resfinal_query1.tables(2).tableAlias_name == "TBL_TRES")) {
        resultado_final = false
        println(s"error: tabla(2) real/esperado database_name [${resfinal_query1.tables(2).database_name}]/[PRODUCTION_MASTER], table_name [${resfinal_query1.tables(2).table_name}]/[TBL_TRES], database_name [${resfinal_query1.tables(2).tableAlias_name}]/[TBL_TRES]")
      }
      return resultado_final
    }
      
    def testplan_query01_T005(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(3).database_name == null && 
          resfinal_query1.tables(3).table_name == "TABLAORIGEN" &&
          resfinal_query1.tables(3).tableAlias_name == "ORIG")) {
        resultado_final = false
        println(s"error: tabla(3) real/esperado database_name [${resfinal_query1.tables(3).database_name}]/[null], table_name [${resfinal_query1.tables(3).table_name}]/[TABLAORIGEN], database_name [${resfinal_query1.tables(3).tableAlias_name}]/[ORIG]")
      }
      return resultado_final
    }
      
    def testplan_query01_T006(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(4).database_name == "TEMP_HUEMUL" && 
          resfinal_query1.tables(4).table_name == "TEMP_HUEMUL_3" &&
          resfinal_query1.tables(4).tableAlias_name == "OTRA")) {
        resultado_final = false
        println(s"error: tabla(4) real/esperado database_name [${resfinal_query1.tables(4).database_name}]/[TEMP_HUEMUL], table_name [${resfinal_query1.tables(4).table_name}]/[TEMP_HUEMUL_3], database_name [${resfinal_query1.tables(4).tableAlias_name}]/[OTRA]")
      }
      return resultado_final
    }
    
    def testplan_query01_T007(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(5).database_name == "TEMP_HUEMUL" && 
          resfinal_query1.tables(5).table_name == "TEMP_HUEMUL_5" &&
          resfinal_query1.tables(5).tableAlias_name == "MAE")) {
        resultado_final = false
        println(s"error: tabla(5) real/esperado database_name [${resfinal_query1.tables(5).database_name}]/[TEMP_HUEMUL], table_name [${resfinal_query1.tables(5).table_name}]/[TEMP_HUEMUL_5], database_name [${resfinal_query1.tables(5).tableAlias_name}]/[MAE]")
      }
      return resultado_final
    }
      
    def testplan_query01_T008(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(6).database_name == "PRODUCTION_DIM" && 
          resfinal_query1.tables(6).table_name == "TBL_LEFT_JOIN" &&
          resfinal_query1.tables(6).tableAlias_name == "TBL_LEFT_JOIN")) {
        resultado_final = false
        println(s"error: tabla(6) real/esperado database_name [${resfinal_query1.tables(6).database_name}]/[PRODUCTION_DIM], table_name [${resfinal_query1.tables(6).table_name}]/[TBL_LEFT_JOIN], database_name [${resfinal_query1.tables(6).tableAlias_name}]/[TBL_LEFT_JOIN]")
      }
      return resultado_final
    }  
      
    def testplan_query01_T009(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.tables(7).database_name == null && 
          resfinal_query1.tables(7).table_name == "TBL_FULL_JOIN" &&
          resfinal_query1.tables(7).tableAlias_name == "FJULL")) {
        resultado_final = false
        println(s"error: tabla(7) real/esperado database_name [${resfinal_query1.tables(7).database_name}]/[null], table_name [${resfinal_query1.tables(7).table_name}]/[TBL_FULL_JOIN], database_name [${resfinal_query1.tables(7).tableAlias_name}]/[FJULL]")
      }
      return resultado_final
    }  
    
    
    
    
    def testplan_query01_SQRes(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.subquery_result.length == 2)){
        resultado_final = false
        println(s"error: subquery, esperado 2, real ${resfinal_query1.subquery_result.length} ")
      }
      return resultado_final
    }  
    
    def testplan_query01_SQ_001_T001(): Boolean = {
      var resultado_final: Boolean = true
     
      if (!(resfinal_query1.subquery_result(0).tables(0).database_name == null && 
          resfinal_query1.subquery_result(0).tables(0).table_name == "TABLA3" &&
          resfinal_query1.subquery_result(0).tables(0).tableAlias_name == "TABLA3")) {
        resultado_final = false
        println(s"error: subquery(0) tabla(0) real/esperado database_name [${resfinal_query1.subquery_result(0).tables(0).database_name}]/[null], table_name [${resfinal_query1.subquery_result(0).tables(0).table_name}]/[TABLA3], database_name [${resfinal_query1.subquery_result(0).tables(0).tableAlias_name}]/[TABLA3]")
      }
      return resultado_final
    }  
    
    def testplan_query01_SQ_001_T001_C01(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.subquery_result(0).columns(0).column_name == "CAMPO1" && 
          resfinal_query1.subquery_result(0).columns(0).column_sql == """campo1""" &&
          resfinal_query1.subquery_result(0).columns(0).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(0) real/esperado column_name [${resfinal_query1.subquery_result(0).columns(0).column_name}]/[CAMPO1], column_sql [${resfinal_query1.subquery_result(0).columns(0).column_sql}]/[campo1], column_origin [${resfinal_query1.subquery_result(0).columns(0).column_origin.length}]/[1]""")
      } else {
        if (!(resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_database_name == null &&
              resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_table_name == null &&
              resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_tableAlias_name == null &&
              resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_column_name == "CAMPO1")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_database_name}]/[null], 
                      trace_table_name [${resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_table_name}]/[null], 
                      trace_tableAlias_name [${resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_tableAlias_name}]/[null], 
                      trace_column_name [${resfinal_query1.subquery_result(0).columns(0).column_origin(0).trace_column_name}]/[CAMPO1]  """)        
        }
      }
      return resultado_final
    }
    
    def testplan_query01_SQ_001_T001_C02(): Boolean = {
      var resultado_final: Boolean = true
      
      if (!(resfinal_query1.subquery_result(0).columns(1).column_name == "CAMPO2" && 
          resfinal_query1.subquery_result(0).columns(1).column_sql == """campo2""" &&
          resfinal_query1.subquery_result(0).columns(1).column_origin.length == 1 
          )) {
        resultado_final = false
        println(s"""error: tabla(0) campo(0) real/esperado column_name [${resfinal_query1.subquery_result(0).columns(1).column_name}]/[CAMPO2], column_sql [${resfinal_query1.subquery_result(0).columns(1).column_sql}]/[campo2], column_origin [${resfinal_query1.subquery_result(0).columns(1).column_origin.length}]/[1]""")
      } else {
        if (!(resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_database_name == null &&
              resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_table_name == null &&
              resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_tableAlias_name == null &&
              resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_column_name == "CAMPO2")) {
          resultado_final = false
          println(s"""error: tabla(0) campo(0) origin(0) real/esperado 
                      trace_database_name [${resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_database_name}]/[null], 
                      trace_table_name [${resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_table_name}]/[null], 
                      trace_tableAlias_name [${resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_tableAlias_name}]/[null], 
                      trace_column_name [${resfinal_query1.subquery_result(0).columns(1).column_origin(0).trace_column_name}]/[CAMPO2]  """)        
        }
      }
      return resultado_final
    }

}
