package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def ST_Contains(rectangleQuery: String, point: String): Boolean = {
    if (rectangleQuery == null || point == null || rectangleQuery.isEmpty() || point.isEmpty())
      return false
    val rectangleQArray = rectangleQuery.split(",")
    val pointQArray = point.split(",")

    val rx1 = rectangleQArray(0).toDouble
    val ry1 = rectangleQArray(1).toDouble
    val rx2 = rectangleQArray(2).toDouble
    val ry2 = rectangleQArray(3).toDouble

    val x1 = pointQArray(0).toDouble
    val y1 = pointQArray(1).toDouble

    if(x1<=rx2 && x1>=rx1 && y1>=ry1 && y1<=ry2) return true
    else if(x1>=rx2 && x1<=rx1 && y1<=ry1 && y1>=ry2) return true
    else return false
  }

  def ST_Within(pointString1:String, pointString2:String, distance:Double): Boolean =
  {
    if (pointString1 == null || pointString2 == null || pointString1.isEmpty() || pointString2.isEmpty())
      return false

    val pString1 = pointString1.split(",")
    val pString2 = pointString2.split(",")
    val m = pString1(0).toDouble
    val n = pString1(1).toDouble
    val p = pString2(0).toDouble
    val q = pString2(1).toDouble

    val euclidean_dist_value = math.sqrt(math.pow(m - p, 2) + math.pow(n - q, 2))
    if (euclidean_dist_value > distance)
      return false
    else
      return true
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(ST_Contains(queryRectangle,pointString)))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>(ST_Within(pointString1, pointString2, distance)))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
