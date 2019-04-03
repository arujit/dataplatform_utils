package com.goibibo.dp.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.commons.lang3.exception.ExceptionUtils
import org.apache.zookeeper.Watcher.Event.{EventType, KeeperState}
import org.apache.zookeeper.ZooDefs.Ids
import org.apache.zookeeper._
import org.apache.zookeeper.data.Stat
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.JavaConversions._
import scala.util.{Failure, Success, Try}

object ZkUtils {
    private val logger: Logger = LoggerFactory.getLogger(ZkUtils.getClass)

    def connect(conStr: String, zkSessionTimeout: Int = 3000): Option[ZooKeeper] = {
        val connSignal = new CountDownLatch(1)
        Try {
            val zk = new ZooKeeper(conStr, zkSessionTimeout, countDownOnceConnected(connSignal))
            if (connSignal.await(zkSessionTimeout, TimeUnit.MILLISECONDS)) Some(zk) else None
        }.getOrElse(None)
    }

    def close(implicit zk: ZooKeeper): Boolean = {
        Try {
            zk.close()
        }.isSuccess
    }

    def createNode(createMode: CreateMode, path: String, data: Array[Byte], failSilently: Boolean)(implicit zk: ZooKeeper): Boolean = {
        Try {
            zk.create(path, data, Ids.OPEN_ACL_UNSAFE, createMode)
        } match {
            case Success(newNodepath: String) =>
                logger.info(s"created $createMode node $newNodepath")
                true
            case Failure(e: Throwable) =>
                if (!failSilently) {
                    logger.warn(s"Error in creating $createMode node $path ${ExceptionUtils.getStackTrace(e)}")
                }
                false
        }
    }

    def createNode(createMode: CreateMode, path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean = {
        createNode(createMode, path, data, failSilently = false)
    }

    def createNode(createMode: CreateMode, path: String, data: String, failSilently: Boolean)(implicit zk: ZooKeeper): Boolean =
        createNode(createMode, path, data.getBytes, failSilently)

    def createNode(createMode: CreateMode, path: String, data: String)(implicit zk: ZooKeeper): Boolean =
        createNode(createMode, path, data.getBytes)

    def createEphemeralSequentialNode(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean =
        createNode(CreateMode.EPHEMERAL_SEQUENTIAL, path, data)

    def createEphemeralSequentialNode(path: String, data: String)(implicit zk: ZooKeeper): Boolean =
        createEphemeralSequentialNode(path, data.getBytes)

    def createEphemeralNode(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean =
        createNode(CreateMode.EPHEMERAL, path, data)

    def createEphemeralNode(path: String, data: String)(implicit zk: ZooKeeper): Boolean =
        createEphemeralNode(path, data.getBytes)

    def createPersistentSequentialNode(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean =
        createNode(CreateMode.PERSISTENT_SEQUENTIAL, path, data)

    def createPersistentSequentialNode(path: String, data: String)(implicit zk: ZooKeeper): Boolean =
        createPersistentSequentialNode(path, data.getBytes)

    def createPersistentNode(path: String, data: Array[Byte], failSilently: Boolean)(implicit zk: ZooKeeper): Boolean =
        createNode(CreateMode.PERSISTENT, path, data, failSilently)

    def createPersistentNode(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean =
        createNode(CreateMode.PERSISTENT, path, data)

    def createPersistentNode(path: String, data: String, failSilently: Boolean)(implicit zk: ZooKeeper): Boolean =
        createPersistentNode(path, data.getBytes, failSilently)

    def createPersistentNode(path: String, data: String)(implicit zk: ZooKeeper): Boolean =
        createPersistentNode(path, data.getBytes)

    /**
      * Return true if node exists otherwise false. In case of failures None is returned.
      *
      * @param path zookeeper node path
      * @param zk   zookeeper client
      * @return
      */
    def nodeExists(path: String)(implicit zk: ZooKeeper): Option[Boolean] = {
        Try {
            zk.exists(path, false)
        } match {
            case Success(result) =>
                Some(result != null)
            case Failure(e: Throwable) =>
                logger.warn(s"Error in getting data from $path ${ExceptionUtils.getStackTrace(e)}")
                None
        }
    }

    def getNodeData(path: String)(implicit zk: ZooKeeper): Option[Array[Byte]] = {
        Try {
            zk.getData(path, false, null)
        } match {
            case Success(result) =>
                logger.debug(s"Received data for path $path")
                Some(result)
            case Failure(e: Throwable) =>
                logger.warn(s"Error in getting data from $path ${ExceptionUtils.getStackTrace(e)}")
                None
        }
    }

    def getNodeDataAsString(path: String)(implicit zk: ZooKeeper): Option[String] = {
        getNodeData(path).map {
            new String(_)
        }
    }

    def updateNode(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean = {
        Try {
            zk.setData(path, data, zk.exists(path, true).getVersion)
        } match {
            case Success(newNodepath: Stat) =>
                logger.info(s"set the data for node $newNodepath")
                true
            case Failure(e: Throwable) =>
                logger.warn(s"Error in setting data for node $path ${ExceptionUtils.getStackTrace(e)}")
                false
        }
    }

    /**
      * Update the given node with data. If node doesn't exist then it will create the node and set the value.
      *
      * @param path zookeeper node path
      * @param zk   zookeeper client
      * @return
      */
    def upsert(path: String, data: Array[Byte])(implicit zk: ZooKeeper): Boolean = {
        if (nodeExists(path).isDefined && nodeExists(path).get)
            updateNode(path, data)
        else if (nodeExists(path).isDefined && !nodeExists(path).get)
            createPersistentNode(path, data)
        else false
    }

    def deleteNodeR(path: String)(implicit zk: ZooKeeper): Unit = {
        getChildren(path) match {
            case Some(children: Seq[String]) => children.foreach { child => deleteNodeR(path + "/" + child) }
            case _ =>
        }
        deleteNode(path)
    }

    def deleteNode(path: String)(implicit zk: ZooKeeper): Boolean = {
        Try {
            zk.delete(path, zk.exists(path, true).getVersion)
        } match {
            case Success(_) =>
                logger.info(s"delete node $path succeeded!")
                true
            case Failure(e: Throwable) =>
                logger.warn(s"Error in delete node $path ${ExceptionUtils.getStackTrace(e)}")
                false
        }
    }

    def getChildren(path: String)(implicit zk: ZooKeeper): Option[Seq[String]] = {
        Try {
            zk.getChildren(path, false)
        } match {
            case Success(result) =>
                logger.info(s"Found children $result")
                Some(result.toSeq)
            case Failure(e: Throwable) =>
                logger.warn(s"Error in getChildren $path ${ExceptionUtils.getStackTrace(e)}")
                None
        }
    }

    def callbackWhenNodeGetsDeleted(path: String, callback: () => Unit)(implicit zk: ZooKeeper): Boolean = {
        Try {
            zk.getData(path, createOnDeleteNotifyWatcher(callback), null)
        } match {
            case Success(_) =>
                logger.info(s"Set the watch for $path")
                true
            case Failure(e: Throwable) =>
                logger.warn(s"Error setting the watch on $path ${ExceptionUtils.getStackTrace(e)}")
                false
        }
    }

    def createOnDeleteNotifyWatcher(callback: () => Unit): Watcher = {
        new Watcher {
            def process(event: WatchedEvent): Unit =
                if (event.getType == EventType.NodeDeleted) callback()
        }
    }

    private def countDownOnceConnected(connSignal: CountDownLatch): Watcher = {
        new Watcher {
            def process(event: WatchedEvent): Unit =
                if (event.getState == KeeperState.SyncConnected) connSignal.countDown()
        }
    }
}

/*

import com.goibibo.dp.utils.ZkUtils
implicit val zk = ZkUtils.connect("127.0.0.1:2181/apps/etl_data").get
ZkUtils.createEphemeralNode("/nodeE","ephemeral node")
ZkUtils.createEphemeralSequentialNode("/nodeES","ephemeral sequential node 1")
ZkUtils.createEphemeralSequentialNode("/nodeES","ephemeral sequential node 2")
ZkUtils.createPersistentNode("/etl_production","persistent /etl_production")
ZkUtils.createPersistentSequentialNode("/etl_production/nodeEP","persistent ephemeral node in /etl_production 1")
ZkUtils.createPersistentSequentialNode("/etl_production/nodeEP","persistent ephemeral node in /etl_production 2")
ZkUtils.close

implicit val zk = ZkUtils.connect("127.0.0.1:2181/etl_data").get
ZkUtils.getChildren("/etl_production")
ZkUtils.getChildren("/etl_production").get.foreach{ nodeName => ZkUtils.deleteNode("/etl_production/" + nodeName) }
ZkUtils.deleteNode("/etl_production")
ZkUtils.close

*/