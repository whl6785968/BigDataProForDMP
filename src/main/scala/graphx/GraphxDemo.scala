package graphx

import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object GraphxDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setMaster("local[4]")
    conf.setAppName(s"${this.getClass.getSimpleName}")

    val sc = new SparkContext(conf)

    val vertex : RDD[(VertexId,(String,Int))] = sc.parallelize(Seq(
      (1, ("涛涛", 18)),
      (2, ("大鹏", 28)),
      (9, ("林冲", 38)),
      (6, ("静静", 18)),
      (133, ("翔哥", 24)),

      (16, ("帅帅", 16)),
      (21, ("徐峰", 48)),
      (44, ("腾云", 25)),
      (138, ("田田", 19)),

      (5, ("猛哥", 40)),
      (7, ("刘泽", 38)),
      (158, ("四海", 27))

    ))

    val edge :RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1, 133, 0),
      Edge(2, 133, 0),
      Edge(6, 133, 0),
      Edge(9, 133, 0),

      Edge(16, 138, 0),
      Edge(6, 138, 0),
      Edge(21, 138, 0),
      Edge(44, 138, 0),

      Edge(5, 158, 0),
      Edge(7, 158, 0)
    ))

    val graph =Graph(vertex,edge)

    //(16,1) (2,1) ... (158,5) (7,5)
    val vertices = graph.connectedComponents().vertices

    /*vertices.foreach(println)*/

    vertices.join(vertex).map {
      case (vid, (cid, (name, age))) => (cid,List(name))
    }.reduceByKey(_ ++ _).foreach(println)

    sc.stop()

  }



}
