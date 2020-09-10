package com.itheima.main

import com.itheima.bean.{WebLogBean, WeblogBeanCase}
import com.itheima.service.PageViewService
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
 * 需求：（优化后解决方案，解决数据倾斜问题）
 * 获取flume采集到hdfs的点击流日志，将点击流日志进行ETL操作
 * 同时将ETL后的数据写入到hive的ods层
 * 1）写入清洗后的点击流日志到weblog_origin表
 * 2）点击流模型pageviews表
 * 3）点击流visit模型表
 */
object OptmizeETLApp {
  def main(args: Array[String]): Unit = {
    //初始化sparksession对象
    val conf = new SparkConf()
    val spark: SparkSession = SparkSession.builder().master("local[*]").config(conf).appName("etl").getOrCreate()

    //获取上下文对象
    val sc: SparkContext = spark.sparkContext

    //E:\广州黑马云计算大数据就业7期（20200324双元）\千亿级数仓-离线数仓\day04\资料\数据预处理\日志数据
    //val textRDD: RDD[String] = sc.textFile("/spark_etl/data/input4")
    val textRDD: RDD[String] = sc.textFile("./data/input")

    //读取日志数据，解析字符串转换成bean对象
    /**
     * 1：过滤掉静态请求资源路径
     * 2：按照用户id进行分组，生成sessionid
     * 3：生成pageviews模型
     * 4：生成visit模型
     */
    val weblogBeanRdd: RDD[WebLogBean] = textRDD.map(WebLogBean(_))

    //过滤掉请求失败的资源
    val filterWeblogBeanRdd: RDD[WebLogBean] = weblogBeanRdd.filter(
      x => {
        x != null && x.valid
      }
    )

    //过滤掉静态资源
    initlizePages()
    //使用广播变量，将所有的静态资源数据广播出去
    val pagesBroadcast: Broadcast[mutable.HashSet[String]] = sc.broadcast(pages)

    //将静态资源的请求过滤掉
    val filterStaticWeblogRDD: RDD[WebLogBean] = filterWeblogBeanRdd.filter(
      bean => {
        //获取到请求的资源地址
        val request: String = bean.request
        //如果当前的请求在静态资源列表中存在，则当前请求是静态资源，因此过滤掉
        val res: Boolean = pagesBroadcast.value.contains(request)
        if (res) {
          false
        } else {
          true
        }
      }
    )

    import  spark.implicits._
    //清洗过滤过程，因此可以将数据保存到weblog_origin
    //定义hive表的对象，字段及字段类型需要一一匹配，否则加载数据失败，因此定义样例类：WeblogBeanCase，WebLogBean-》WebLogBean才可以写入hdfs
    val weblogBeanCaseDataSet = filterStaticWeblogRDD.map(bean => WeblogBeanCase(bean.valid, bean.remote_addr, bean.remote_user, bean.time_local,
      bean.request, bean.status, bean.body_bytes_sent, bean.http_referer, bean.http_user_agent, bean.guid)).toDS()

    PageViewService.savePageViewToHdfs(filterStaticWeblogRDD)

    sc.stop()
  }

  //我们准备一个静态资源的集合
  // 用来存储网站url分类数据
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
