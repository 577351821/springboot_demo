package com.itheima.service

import java.util
import java.util.UUID

import cn.itcast.bean.WebLogBean
import cn.itcast.utils.DateUtil
import org.apache.spark.RangePartitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
 * pageview模型实现
 */
object PageViewService {

  /**
   * 将pageview模型数据保存到hdfs中
   *
   * @param filterStaticWeblogRDD
   */
  def savePageViewToHdfs(filterStaticWeblogRDD: RDD[WebLogBean]): Unit = {
    /**
     * 1：将groupby的key由guid改成guid+timelocal字段组成
     * 2：使用rangepartitioner进行数据的均匀分区
     */
    val uidTimelocalRDD: RDD[(String, WebLogBean)] = filterStaticWeblogRDD.map(bean => (bean.guid + "&" + bean.time_local, bean))
    //为了保证key全局有序的排序使用sortBykey进行排序
    val sortedUidTimelocalRDD: RDD[(String, WebLogBean)] = uidTimelocalRDD.sortByKey()
    //使用rangePartition对rdd进行划分为100个分区
    val rangeRDD: RDD[(String, WebLogBean)] = sortedUidTimelocalRDD.partitionBy(new RangePartitioner(100, sortedUidTimelocalRDD))

    //定义累加器收集每个分区的首尾记录，目的是为了判断跨分区存储的某个用户的访问时间差以及是否属于同一个sessionid
    val spark: SparkSession = SparkSession.getDefaultSession.get
    //使用集合类型的累加器收集分区的首尾记录
    val headTailList: CollectionAccumulator[(String, String)] = spark.sparkContext.collectionAccumulator[(String, String)]("headTailList")
    val sessionidRDD: RDD[(WebLogBean, String, Int, Long)] = generateSessionid(rangeRDD, headTailList)

    //累加器中的数据必须要触发计算，使用累加器的时候一定要注意重复计算的问题
    //对rdd数据进行cache防止重复计算
    sessionidRDD.cache()
    //触发一次计算
    sessionidRDD.count()

    val headTailListValue: util.List[(String, String)] = headTailList.value

    //累加器的数据是放在了java的集合列表中，因此需要将数据转换成scala集合
    import collection.JavaConverters._
    val headTailListBuffer: mutable.Buffer[(String, String)] = headTailListValue.asScala
    //将buffer转换成map对象，方便使用（操作）
    val headtailMap = mutable.HashMap(headTailListBuffer.toMap.toSeq: _*)
    //根据首尾数据判断边界问题，得到正确的数据
    val currentMap: mutable.HashMap[String, String] = processBoundaryMap(headtailMap)

    //修复数据
    //currentMap.foreach(println(_))
    //将处理后的每个分区的边界数据广播到每个task节点
    val questionBroadcast: Broadcast[mutable.HashMap[String, String]] = spark.sparkContext.broadcast(currentMap)
    //根据广播后的分区边界数据修复数据
    val pageViewRDD: RDD[(WebLogBean, String, Int, Long)] = repairBoundarySession(sessionidRDD, questionBroadcast)

//    import spark.implicits._
//    val weblogBeanCaseDataSet: RDD[WeblogBeanCase] = pageViewRDD.map(bean => WeblogBeanCase(bean._1.valid, bean._1.remote_addr, bean._1.remote_user, bean._1.time_local,
//      bean._1.request, bean._1.status, bean._1.body_bytes_sent, bean._1.http_referer, bean._1.http_user_agent, bean._1.guid))
//
//    weblogBeanCaseDataSet.toDS.write.mode(SaveMode.Overwrite).parquet("/user/hive/warehouse/itcast_ods.db/weblog_origin/dt=20191101/")

    sessionidRDD.saveAsTextFile("./data/output/pageviewtext")
    pageViewRDD.saveAsTextFile("./data/output/pageviewtext2")
    sessionidRDD.unpersist()
  }

  //修复我们rdd边界处的数据
  def repairBoundarySession(uidTimeSessionStepLongRdd: RDD[( WebLogBean, String, Int, Long)],
                            questionBroadCast: Broadcast[mutable.HashMap[String, String]]) = {
    //key:index/first/last,value:last-->timediff,first-->correctsessionid+correctstep+quesitonsessionid
    val questionMap: mutable.HashMap[String, String] = questionBroadCast.value
    val correctRdd: RDD[(WebLogBean, String, Int, Long)] = uidTimeSessionStepLongRdd.mapPartitionsWithIndex(
      (index, iter) => {
        //uid&time,sessionid,step,staylong
        var orginList = iter.toList
        val firstLine: String = questionMap.getOrElse(index + "&first", "")
        val lastLine: String = questionMap.getOrElse(index + "&last", "")
        if (lastLine != "") {
          //当前这个分区最后一条数据他的停留时长需要修改
          val buffer: mutable.Buffer[(WebLogBean, String, Int, Long)] = orginList.toBuffer
          val lastTuple: (WebLogBean, String, Int, Long) = buffer.remove(buffer.size - 1) //只修改停留时长
          buffer += ((lastTuple._1, lastTuple._2, lastTuple._3, lastLine.toLong))
          orginList = buffer.toList
        }

        if (firstLine != "") {
          //分区第一条数据有问题，则需要修改：按照错误的sessionid找到所有需要修改的数据，改正sessionid和step
          val firstArr: Array[String] = firstLine.split("&")
          val tuples: List[(WebLogBean, String, Int, Long)] = orginList.map {
            t => {
              if (t._2.equals(firstArr(2))) {
                //错误的sessionid,修改为正确的sessionid和步长
                (t._1, firstArr(0), firstArr(1).toInt + t._3.toInt, t._4)
              } else {
                t
              }
            }
          }
          orginList=tuples
        }
        orginList.iterator
      }
    )
    correctRdd

  }

  /**
   * 根据首尾数据找到有边界问题的数据，修改成正确的数据
   * @param headtailMap
   */
  def processBoundaryMap(headtailMap: mutable.HashMap[String, String]) = {
    //定义一个map接受有问题的分区需要修改的数据：key：index+"&frist/last", value:需要修改的正确数据
    val correntMap = new mutable.HashMap[String, String]()
    //因为每个分区的首尾记录存储到了headtaillist集合中，因此只需要循环size/2即可找到首尾两条记录
    for(num <-1 until( headtailMap.size/2)){
      //根据num分区获取对应的首尾记录
      val numFirstMsg: String = headtailMap.get(num+"&first").get //找到分区内的首条记录，uid+&+time+&+sessionid
      val numLastMsg: String = headtailMap.get(num+"&last").get //找到分区内的尾条记录，uid+&+time+&+sessionid+&+step+&+partition.size(分区数据数量)

      //获取上一个分区的最后一条记录
      val prePartLastMsg: String = headtailMap.get((num - 1) + "&last").get
      //判断上一个分区和当前分区是否存在边界问题
      val numLastArr: Array[String] = numLastMsg.split("&")
      val prePartLastArr = prePartLastMsg.split("&")
      val numFirstArr: Array[String] = numFirstMsg.split("&")
      //判断是否是同一个用户
      if(prePartLastArr(0).equals(numFirstArr(0))){
        //判断两者之间的时间差
        val timediff: Long = DateUtil.getTimeDiff(prePartLastArr(1), numFirstArr(1))
        if(timediff< 30* 60 * 1000){
          //说明当前分区的第一条数据与上一个分区的最后一条数据属于同一个会话
          //上个分区记录需要修改的争取的停留时长数据
          //上一个分区最后一条记录的时间差应该改成跟下个分区首条记录的真实的时间差：60000-》实际的时间差
          correntMap.put((num-1)+"&last", timediff.toString)

          //TODO 处理每个分区的首条记录
          //说明当前分区的第一条记录（有可能是多条记录的第一个session的数据）需要修改的数据
          //if(prePartLastArr.size)
          //修改当前分区的首条记录的数据
          //currentTuple._1 + "&" + sessionId + "&" + step + "&" + list.size
          //修改后的首条记录字段应该包含正确的sessionid、以及正确的step（上个分区尾条记录的step+1）
          if(prePartLastArr.size>5){
            correntMap.put(num+"&first", prePartLastArr(prePartLastArr.size-2)+"&"+prePartLastArr(prePartLastArr.size-1)+"&"+numFirstArr(2))
          }else {
            correntMap.put(num + "&first", prePartLastArr(2) + "&" + prePartLastArr(3) + "&" + numFirstArr(2))
          }
          //TODO 处理每个分区的尾条记录
          //修改后的尾条记录字段应该包含正确的页面停留时间（60000s）-》尾条数据的访问时间与下个分区的首条记录的访问时间的时间差
          //判断当前分区是否是同一个会话
          if(numFirstArr(2).equals(numLastArr(2))) {
            //说明当前分区是同一个会话，存在会话穿透多个分区的现象
            //更新最后一条数据的stephe sessionid信息
            //numLastMsg + 正确的sessionid(上个分区的最后一条数据的sesionid)+正确的步长step(上个分区最后一条数据的步长+当前分区的数据数量)
            //(23&first,18cef1b5-c7cd-4af0-a70c-52db773fd76f&2019-11-01 20:08:57&8e3c1827-c062-440b-989d-f605a5fa8df9)
            //(30&last,3cacf776-cd86-4b8c-8544-96e7fe7e979a&2019-11-01 03:17:27&60979270-db7b-490e-b00c-f51ec7846e77&22&22)
            //correntMap.put(num+"&last", numLastMsg+"&"+prePartLastArr(2)+"&"+(prePartLastArr(3).toInt+numLastArr(4).toInt))
            if (prePartLastArr.size > 5) {
              //修改过的分区数据了
              headtailMap.put(num + "&last", numLastMsg + "&" + prePartLastArr(prePartLastArr.size - 2) + "&" + (prePartLastArr.size - 1) + numLastArr(4).toInt)
            } else {
              //numLastMsg(1,5)
              //numLastMsg(1,5)&0&10
              headtailMap.put(num + "&last", numLastMsg + "&" + prePartLastArr(2) + "&" + (prePartLastArr(3).toInt + numLastArr(4).toInt))
            }
          }
        }
      }
    }
    correntMap
  }

  /**
   * 对均匀分区的rdd生成sessionid，使用累加器收集每个分区的首尾记录数据
   * @param rangeRDD
   * @param headTailList
   */
  def generateSessionid(rangeRDD: RDD[(String, WebLogBean)], headTailList: CollectionAccumulator[(String, String)]) = {
    val sessionidPageRDD: RDD[(WebLogBean, String, Int, Long)] = rangeRDD.mapPartitionsWithIndex {
      (index, iter) => {
        //获取某个分区下的所有数据，将获取到的集合数据转换成list对象存储
        val list = iter.toList
        //两两比较，生成sessionid、step、staylone（停留时长），停留时长的计算：根据后一条数据的访问时间减去前一条的方式时间得到停留时长
        val pagesViewBeanCaseList: ListBuffer[(WebLogBean, String, Int, Long)] = new ListBuffer[(WebLogBean, String, Int, Long)]()
        //准备sessionid、setp、pagestaylone
        //sessionid，自定义的会话id
        var sessionId: String = UUID.randomUUID().toString()
        //定义当前会话内的第几步
        var step = 1
        //每个页面的停留时间，默认60s
        val page_staylong = 60000
        import scala.util.control.Breaks._
        breakable {
          for (num <- 0 until (list.size)) {
            //获取当前遍历的数据
            val currentTuple: (String, WebLogBean) = list(num)
            //如果num==0表示分区的第一条数据。因此将第一条数据放入到累加器
            if (num == 0) {
              //将数据转载到累加器中，key：分区编号&first/last，value：uid+timelocal，sessionid
              headTailList.add((index + "&first", currentTuple._1 + "&" + sessionId))
            }
            //如果只有一条数据的情况下
            if (list.size == 1) {
              //当前分区只有一条数据，不需要生成pageviewbeancase类型的数据
              pagesViewBeanCaseList += ((currentTuple._2, sessionId, step, page_staylong))
              //重新生成sessionid
              sessionId = UUID.randomUUID().toString
              //跳出循环
              break()
            }
            //不止一条数据的情况下
            //第一条数据跳过，从第二条数据开始遍历
            breakable {
              if (num == 0) {
                //说明是第一条记录，跳过
                break()
              }
              //从第二条开始判断
              //获取到上一条的数据进行两两比较
              val lastTuple: (String, WebLogBean) = list(num - 1)
              //获取到生成sessionid需要的uid和timelocal字段
              val currentUidTimelocal: String = currentTuple._1
              //找到上一条数据的uid和timelocal
              val lastUidTime: String = lastTuple._1

              //取出来uid和time：uid&timelocal
              val currentUidTimeArr: Array[String] = currentUidTimelocal.split("&")
              val lastUidTimeArr: Array[String] = lastUidTime.split("&")

              //判断两次访问的时间差
              val timeDiff: Long = DateUtil.getTimeDiff(lastUidTimeArr(1), currentUidTimeArr(1))

              //判断是否是同一个用户
              if (lastUidTimeArr(0).equals(currentUidTimeArr(0)) && timeDiff < 30 * 60 * 1000) {
                //说明两条记录是同一个sessionid，保存上一条数据：sessionid，step，timediff
                pagesViewBeanCaseList += ((lastTuple._2, sessionId, step, timeDiff))
                //sessionid和step的处理，sessionid不需要重新生成，step+1
                step += 1
              } else {
                //说明两条数据是不同的会话
                pagesViewBeanCaseList += ((lastTuple._2, sessionId, step, page_staylong))
                //重新生成sessionid
                sessionId = UUID.randomUUID().toString()
                //step重置
                step = 1
              }

              //如果是尾条数据的情况下
              if (num == list.size - 1) {
                //需要保存最后一条数据
                pagesViewBeanCaseList += ((currentTuple._2, sessionId, step, page_staylong))
                //使用累加器收集分区内的最后一条数据，key：分区编号&first/last，value：uid+timelocal，sessionid
                //println("【"+currentTuple._1 + "】&【" + sessionId + "】&【" + step + "】&【" + list.size+"】")
                headTailList.add((index + "&last", currentTuple._1 + "&" + sessionId + "&" + step + "&" + list.size))
                sessionId = UUID.randomUUID().toString
              }
            }
          }
        }
        pagesViewBeanCaseList.toIterator
      }

    }
    sessionidPageRDD
  }


}
