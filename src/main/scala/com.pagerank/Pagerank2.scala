package com.pagerank

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Pagerank2 {
  def run(args: Array[String]): Unit = {
    //            在本地运行需要加上setMaster("local")，由于最后要放到集群上跑，所以这里去掉了setMster("local")
    val conf = new SparkConf().setAppName("Page Rank cit-Parent file");
    val sc = new SparkContext(conf)

    val data = sc.textFile(args(0))

    val links = data.filter(line=>(!line.startsWith("#"))).map(s=>s.split("\\s+")).map(ids=>(ids(0),ids(1))).distinct().groupByKey()

    var ranks = links.mapValues(v => 1.0)

    val numIter = 10


    for (i <- 1 to numIter) {

      val infopages = links.join(ranks)

      val inforanks = infopages.values.flatMap{ case (links, rank) => links.map(link => (link, rank / links.size)) }

      ranks = inforanks.reduceByKey((x,y)=> x+y).mapValues(value=> value*0.85  + 0.15)
    }

    // sort result pages by rank
    val result = ranks.map(_.swap).sortByKey(false).map(_.swap)
    result.saveAsTextFile(args(1))
    /* 步骤3：关闭SparkContext */
    sc.stop()
  }

  def main(args: Array[String]): Unit = {
    run(args)
  }
}