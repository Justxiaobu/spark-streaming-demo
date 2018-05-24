package com.spark.demo.utils

import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper.{CreateMode, WatchedEvent, Watcher, ZooKeeper}

/**
  * author sunbin
  * date 2018-05-24 10:14
  * description zookeeper配置工具类
  */
object ZkUtil {

  val TIME_OUT = LoadConfs.getInteger("zk.time_out")
  val CONNECT_PATH = LoadConfs.getProperty("zk.brokers")
  var zooKeeper : ZooKeeper = _

  def watcher = new Watcher {
    override def process(event: WatchedEvent): Unit ={
      println(s"[ ZkWork ] process : " + event.getType)
    }
  }

  /**
    * zookeeper连接
    */
  def connect() {
    println(s"[ ZkWork ] zk connect")
    zooKeeper = new ZooKeeper(CONNECT_PATH, TIME_OUT, watcher)
  }

  /**
    * 创建znode
    * @param znode
    * @param data
    */
  def znodeCreate(znode: String, data: String) {
    println(s"[ ZkWork ] zk create /$znode , $data")
    zooKeeper.create(s"/$znode", data.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
  }

  /**
    * @param znode
    * @param data
    */
  def znodeDataSet(znode: String, data: String) {
    println(s"[ ZkWork ] zk data set /$znode")
    zooKeeper.setData(s"/$znode", data.getBytes(), -1)
  }

  /**
    * 获取znode数据
    * @param znode
    * @return
    */
  def znodeDataGet(znode: String): Array[String] = {
    connect()
    println(s"[ ZkWork ] zk data get /$znode")
    try {
      new String(zooKeeper.getData(s"/$znode", true, null), "utf-8").split(",")
    } catch {
      case _: Exception => Array()
    }
  }

  /**
    * 判断znode是否存在
    * @param znode
    * @return
    */
  def znodeIsExists(znode: String): Boolean ={
    connect()
    println(s"[ ZkWork ] zk znode is exists /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => false
      case _ => true
    }
  }

  /**
    * 更新znode数据
    * @param znode
    * @param data
    */
  def offsetWork(znode: String, data: String) {
    connect()
    println(s"[ ZkWork ] offset work /$znode")
    zooKeeper.exists(s"/$znode", true) match {
      case null => znodeCreate(znode, data)
      case _ => znodeDataSet(znode, data)
    }
    println(s"[ ZkWork ] zk close★★★")
    zooKeeper.close()
  }





}
