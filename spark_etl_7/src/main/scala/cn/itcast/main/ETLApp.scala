package cn.itcast.main

import java.util.UUID

import cn.itcast.bean.{PageViewsBeanCase, VisitBeanCase, WebLogBean, WeblogBeanCase}
import cn.itcast.utils.DateUtil
import org.apache.hive.common.util.DateUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * 需求：
 * 获取flume采集到hdfs的点击流日志，将点击流日志进行ETL操作
 * 同时将ETL后的数据写入到hive的ods层
 * 1）写入清洗后的点击流日志到weblog_origin表
 * 2）点击流模型pageviews表
 * 3）点击流visit模型表
 */
object ETLApp {
  def main(args: Array[String]): Unit = {
      //初始化sparksession对象
    val conf = new SparkConf()
    val spark: SparkSession = SparkSession.builder().master("local[*]").config(conf).appName(ETLApp.getClass.getSimpleName).getOrCreate()

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

    //保存到hdfs，保存为parquet文件
    //weblogBeanCaseDataSet.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/weblog_origin/dt=20191101/")


    //生成pageview模型的数据写入到hive中
    //按照用户id分组（uid，iterable[WeblogBean]）
    val uidWeblogBean: RDD[(String, Iterable[WebLogBean])] = filterStaticWeblogRDD.groupBy(x => x.guid)
    uidWeblogBean.foreach(println(_))
    //对用户的访问记录进行排序
    val pageViewBeanCaseRDD: RDD[PageViewsBeanCase] = uidWeblogBean.flatMap(
      item => {
        //取到用户id
        val uid = item._1
        println(uid)
        //按照用户的访问时间进行排序
        val sortWebLogBeanList: List[WebLogBean] = item._2.toList.sortBy(_.time_local)
        //两两比较，生成sessionid、step、staylone（停留时长），停留时长的计算：根据后一条数据的访问时间减去前一条的方式时间得到停留时长
        val pagesViewBeanCaseList: ListBuffer[PageViewsBeanCase] = new ListBuffer[PageViewsBeanCase]()
        //sessionid，自定义的会话id
        var sessionId: String = UUID.randomUUID().toString()
        //定义当前会话内的第几步
        var step = 1
        //每个页面的停留时间，默认60s
        val page_staylong = 60000

        //循环遍历每个用户的访问记录集合
        for (num <- 0 until sortWebLogBeanList.size) {
          val bean: WebLogBean = sortWebLogBeanList(num)
          //如果只有一条记录
          if (sortWebLogBeanList.size == 1) {
            val pageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, bean.remote_addr, bean.time_local, bean.request, step, page_staylong, bean.http_referer,
              bean.http_user_agent, bean.body_bytes_sent, bean.status, bean.guid)
            //将数据放到集合中保存起来，最后一期输出
            pagesViewBeanCaseList += pageViewsBeanCase
            //重新生成session
            sessionId = UUID.randomUUID().toString
          } else {
            //有多条点击流日志的时候
            if (num == 0) {
              //第一条数据进来了，不处理
            } else {
              //肯定不是第一条数据了，获取到前一条数据
              val preWebLogBean: WebLogBean = sortWebLogBeanList(num - 1)
              //判断当前数据的访问时间与前一条数据的访问的差额(返回值：毫秒数)
              val timeDiff: Long = DateUtil.getTimeDiff(preWebLogBean.time_local, bean.time_local)
              //毫秒数是否小于30分钟
              if (timeDiff <= 30 * 60 * 1000) {
                //属于同一个session，可以共用一个sessionid，输入前一条记录
                val prePageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, preWebLogBean.remote_addr, preWebLogBean.time_local, preWebLogBean.request, step, timeDiff, preWebLogBean.http_referer,
                  preWebLogBean.http_user_agent, preWebLogBean.body_bytes_sent, preWebLogBean.status, preWebLogBean.guid)

                //将前一条数据加入到集合中
                pagesViewBeanCaseList += prePageViewsBeanCase
                //session不需要重新生成，step需要+1
                step += 1
              } else {
                //不属于一个sessionid，生成新的sessionid
                val prePageViewsBeanCase: PageViewsBeanCase = PageViewsBeanCase(sessionId, preWebLogBean.remote_addr, preWebLogBean.time_local, preWebLogBean.request, step, page_staylong, preWebLogBean.http_referer,
                  preWebLogBean.http_user_agent, preWebLogBean.body_bytes_sent, preWebLogBean.status, preWebLogBean.guid)
                //将前一条数据加入到集合中
                pagesViewBeanCaseList += prePageViewsBeanCase
                //sessionid重新生成
                sessionId = UUID.randomUUID().toString
                //step重置为1
                step = 1
              }
              //如果是最后一条数据的话
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

        println(pagesViewBeanCaseList)
        pagesViewBeanCaseList
      }
    )

   // pageViewBeanCaseRDD.toDS().write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/click_pageviews/dt=20191101")

    //生成visit模型的数据写入到hive中
    //按照sessionid分组，（sessionid，Iterable(PageViewsBeanCase)）
    val sessionRdd: RDD[(String, Iterable[PageViewsBeanCase])] = pageViewBeanCaseRDD.groupBy(_.session)

    val visitBeanCaseRDD: RDD[VisitBeanCase] = sessionRdd.map(item => {
      //sessionid
      val sessionid = item._1
      //sessionid对应的访问记录
      val viewsBeanCases: List[PageViewsBeanCase] = item._2.toList.sortBy(_.time_local)

      //将pageview样例类转换成visit样例类
      val beanCase = VisitBeanCase(viewsBeanCases(0).guid, sessionid, viewsBeanCases(0).remote_addr, viewsBeanCases(0).time_local,
        viewsBeanCases(viewsBeanCases.size - 1).time_local, viewsBeanCases(0).request, viewsBeanCases(viewsBeanCases.size - 1).request,
        viewsBeanCases(0).htp_referer, viewsBeanCases.size)
      beanCase
    })

   // visitBeanCaseRDD.toDS().write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/click_stream_visit/dt=20191101")

    visitBeanCaseRDD.collect().foreach(println(_))
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
