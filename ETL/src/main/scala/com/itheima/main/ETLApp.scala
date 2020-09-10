package com.itheima.main

import java.util.UUID

import com.itheima.bean.{PageViewsBeanCase, VisitBeanCase, WebLogBean, WeblogBeanCase}
import com.itheima.utils.DateUtil
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ETLApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()

    val spark = SparkSession
      .builder()
      .config(conf)
      .master("local[6]").appName("ETL")
      .getOrCreate()
    val sc: SparkContext = spark.sparkContext

    val textRdd: RDD[String] = sc.textFile("/spark_etl/data/input1/")
    val webLogBeanRdd = textRdd.map(WebLogBean(_))

    val filterWeblogBeanRdd = webLogBeanRdd.filter(x => {
      x != null && x.valid
    })
    initlizePages()
    val pagesBroadCast = sc.broadcast(pages)
    val filterStaticWeblogRdd = filterWeblogBeanRdd.filter(
      bean => {
        val request = bean.request
        val res = pagesBroadCast.value.contains(request)
        if (res) {
          false
        } else {
          true
        }
      }
    )

    import spark.implicits._

    val weblogBeanCaseDataset = filterStaticWeblogRdd.map(bean => WeblogBeanCase(bean.valid, bean.remote_addr, bean.remote_user, bean.time_local,
      bean.request, bean.status, bean.body_bytes_sent, bean.http_referer, bean.http_user_agent, bean.guid))
      .toDS()

    weblogBeanCaseDataset.write.mode("overwrite")
      .parquet("/user/hive/warehouse/itcast_ods.db/weblog_origin/dt=20191101/")

    val uidWeblogBean = filterStaticWeblogRdd.groupBy(_.guid)

    val pageViewBeanCaseRDD = uidWeblogBean.flatMap(item => {
      val uid = item._1
      val sortWebLogBeanList = item._2.toList.sortBy(_.time_local)
      val pagesViewBeanCaseList: ListBuffer[PageViewsBeanCase] = new ListBuffer[PageViewsBeanCase]()
      var sessionId = UUID.randomUUID().toString()
      var step = 1
      val page_staylong = 60000

      for (num <- 0 until sortWebLogBeanList.size) {
        val bean = sortWebLogBeanList(num)

        if (sortWebLogBeanList.size == 1) {
          val pageViewsBeanCase = PageViewsBeanCase(sessionId, bean.remote_addr, bean.time_local, bean.request, step, page_staylong, bean.http_referer,
            bean.http_user_agent, bean.body_bytes_sent, bean.status, bean.guid)

          pagesViewBeanCaseList += pageViewsBeanCase

          sessionId = UUID.randomUUID().toString
        } else {
          if (num == 0) {

          } else {
            val preWebLogBean = sortWebLogBeanList(num - 1)
            val timeDiff = DateUtil.getTimeDiff(preWebLogBean.time_local, bean.time_local)
            if (timeDiff <= 30 * 60 * 1000) {
              val prePageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, preWebLogBean.remote_addr, preWebLogBean.time_local, preWebLogBean.request, step, timeDiff, preWebLogBean.http_referer,
                preWebLogBean.http_user_agent, preWebLogBean.body_bytes_sent, preWebLogBean.status, preWebLogBean.guid)

              //将前一条数据加入到集合中
              pagesViewBeanCaseList += prePageViewsBeanCase
              //session不需要重新生成，step需要+1
              step += 1
            } else {
              val prePageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, preWebLogBean.remote_addr, preWebLogBean.time_local, preWebLogBean.request, step, page_staylong, preWebLogBean.http_referer,
                preWebLogBean.http_user_agent, preWebLogBean.body_bytes_sent, preWebLogBean.status, preWebLogBean.guid)
              //将前一条数据加入到集合中
              pagesViewBeanCaseList += prePageViewsBeanCase
              //sessionid重新生成
              sessionId = UUID.randomUUID().toString
              //step重置为1
              step = 1
            }
            if (num == sortWebLogBeanList.size - 1) {
              val pageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, bean.remote_addr, bean.time_local, bean.request, step, page_staylong, bean.http_referer,
                bean.http_user_agent, bean.body_bytes_sent, bean.status, bean.guid)
              //将前一条数据加入到集合中
              pagesViewBeanCaseList += pageViewsBeanCase

              //重新生成sessionid
              sessionId = UUID.randomUUID().toString
            }
          }
        }
      }

      for (elem <- pagesViewBeanCaseList) {
        println(elem)
      }

      pagesViewBeanCaseList
    })
//    pageViewBeanCaseRDD.collect().foreach(println(_))
    pageViewBeanCaseRDD.toDS().write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/click_pageviews/dt=20191101")

    val sessionRdd = pageViewBeanCaseRDD.groupBy(_.session)

//    sessionRdd.collect().foreach(println)

    val visitBeanCaseRDD = sessionRdd.map(item => {
      val sessionid = item._1
      val viewsBeanCases: List[PageViewsBeanCase] = item._2.toList.sortBy(_.time_local)
      val beanCase = VisitBeanCase(viewsBeanCases(0).guid, sessionid, viewsBeanCases(0).remote_addr, viewsBeanCases(0).time_local,
        viewsBeanCases(viewsBeanCases.size - 1).time_local, viewsBeanCases(0).request, viewsBeanCases(viewsBeanCases.size - 1).request,
        viewsBeanCases(0).htp_referer, viewsBeanCases.size)

      println(viewsBeanCases)
      beanCase
    })

    visitBeanCaseRDD.toDS().write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/click_stream_visit/dt=20191101")

    sc.stop()
  }

  val pages = new mutable.HashSet[String]()

  //初始化静态资源路径集合
  def initlizePages(): Unit = {
    pages.add("/about")
    pages.add("/black-ip-list/")
    pages.add("/cassandra-clustor/")
    pages.add("/finance-rhive-repurchase/")
    pages.add("/hadoop-family-roadmap/")
    pages.add("/hadoop-hive-intro/")
    pages.add("/hadoop-zookeeper-intro/")
    pages.add("/hadoop-mahout-roadmap/")
  }


}
