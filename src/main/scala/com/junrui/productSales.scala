package com.junrui

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

object productSales {
  def main(args: Array[String]): Unit = {
    //创建SC和HiveContext
    val conf = new SparkConf().setAppName("sales").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //Sql
//    hiveContext.sql("create table t_service(name string)")
//    hiveContext.sql("load data local inpath'd:/service.txt' into table t_service")
//    hiveContext.sql("select * from table_spark").show()
    //hiveContext extends sparkContext，获得mysql的两张表
    loadMysqlTable(hiveContext,"t_product","t_area")
    //读取hive中的数据,获取临时表spark_pages
    getHiveInitData(hiveContext)
    //将从hive中读取的数据与mysql进行关联转换操作，小表在前大表在后
//    hiveContext.sql("select * from spark_pages").show()

    /**
      * 处理地区的级别
      华北地区   	A
      东北地区    B
      华东地区   	B
      华中地区    C
      华南地区    C
      西南地区   	D
      西北地区   	D
      */
    //获得初步四个字段名的表count_Result
    getCountResult(hiveContext)
    //使用case when then else end的SQL语法增加一个字段名
    getAreaLevel(hiveContext)
    //获得每个分区的前三名！！！！使用hive的开窗函数
    //row_number()可以按照每个分区里的顺序会自动生成一个有顺序的字段名
    //这种写法使分区的排序乱了，变成了所有rank的排序
//    hiveContext.sql("select area_level,area_name,product_name,click_num,extend_info, " +
//                    "row_number() over(partition by area_name order by click_num desc) rank " +
//                    "from area_result where rank<4").show(100)
    hiveContext.sql("select area_level,area_name,product_name,click_num,extend_info, " +
      "row_number() over(partition by area_name order by click_num desc) rank " +
      "from area_result having rank<4").show(100)
    //关闭
    sc.stop()
  }

  /**
    * 加载其他数据库的表,写一个工具类
    * String*类型为String的数组，类似于java里的有参(String tableNames..)
    */
  def loadMysqlTable(hiveContext:HiveContext,tableNames:String*): Unit ={
    for(tableName<-tableNames){
      //加载mysql的t_user的表，放入临时表mysql_t_user
      val optionMap1 = Map("url"->"jdbc:mysql://192.168.200.21:3306/project_useraction?user=root&password=1234","dbtable"->tableName)
      val dataFrame1 = hiveContext.read.format("jdbc").options(optionMap1).load()
      dataFrame1.registerTempTable("mysql_"+tableName)
//      dataFrame1.show()
    }
  }
    /**
      * 读取hive中的数据并做一些初始操作
      */
  def getHiveInitData(hiveContext: HiveContext): Unit ={
    //根据需求，需要将同一个城市中的同一件商品的点击量统计出来
    //使用临时表的原因是防止多次加载耗内存拖时间，内容可以拿出来用
    // default数据库的表不用
    hiveContext.sql("select city_id,click_product_id,count(1) click_num from default.t_pages_click_lyx group by city_id,click_product_id").registerTempTable("spark_pages")
  }

  def getCountResult(hiveContext: HiveContext): Unit ={
    hiveContext.sql("select tarea.area_name,tproduct.product_name,sum(spages.click_num) click_num,tproduct.extend_info "+
      "from mysql_t_area tarea left join spark_pages spages on tarea.city_id=spages.city_id "+
      "join mysql_t_product tproduct on tproduct.product_id=spages.click_product_id "+
      "group by tproduct.product_name,tarea.area_name,tproduct.extend_info").registerTempTable("count_result")
  }

  def getAreaLevel(hiveContext: HiveContext): Unit ={
    hiveContext.sql("select case area_name when '华北地区' then 'a'" +
      "when '东北地区' then 'b' " +
      "when '华东地区' then 'b' " +
      "when '华中地区' then 'c' " +
      "when '华南地区' then 'c' " +
      "when '西南地区' then 'd' " +
      "when '西北地区' then 'd' " +
      "end as area_level,* from count_result").registerTempTable("area_result")
  }

}
