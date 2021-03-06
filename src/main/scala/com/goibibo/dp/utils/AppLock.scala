package com.goibibo.dp.utils
import org.slf4j.{Logger, LoggerFactory}
import java.util.concurrent.{CountDownLatch,TimeUnit}

import org.apache.zookeeper.ZooKeeper

/*
//Usage: 
import com.goibibo.dp.utils._
val zk = AppLock.lock("127.0.0.1:2181/etl_data","/production", 15 * 60 * 1000)
AppLock.unlock(zk)
*/

object AppLock {
	private val logger: Logger = LoggerFactory.getLogger(ZkUtils.getClass)

	/*
		Here is how lock works,	
			- If no application is running then it creates an ephemeral node
			- If one application is already running then 
				- it creates an ephemeral node
				- waits for the previous one to finish
			- If One application is running and another application is already waiting then
				- It does not wait, we should exit the application in this case
			- According to the current design it should never happen that more than 1 application is waiting for the lock.
	*/
	def lock(zookeeperConUrl:String, appLocation:String, timeoutMs:Int):Option[ZooKeeper] = {
		val zkO = ZkUtils.connect(zookeeperConUrl)
		if(zkO.isEmpty) { None }
		else {
			implicit val zk = zkO.get
			//Let us create appLocation node if it does not exist
			ZkUtils.createPersistentNode(appLocation,"", true)
			ZkUtils.getChildren(appLocation) match {
				case None => { 
					ZkUtils.close
					None 
				}
				case Some(children:Seq[String]) => {
					if(children.size == 0) { 
						//TODO: Send a metric
						logger.info("Found no previous application running!, Good to go!")
						ZkUtils.createEphemeralSequentialNode(appLocation + "/hotelEtlApp",(new java.sql.Timestamp(System.currentTimeMillis)).toString)
						if(ZkUtils.getChildren(appLocation).size > 1) {
							logger.warn("One etl-application is already running, Exit the app")
							ZkUtils.close
							None
						} else {
							Some(zk) 	
						}
						
					} else {
						//TODO: Send a metric
						logger.warn("One etl-application is already running, Exit the app")
						ZkUtils.close
						None
					} 
				}
			}
		}
	}
	
	def unlock(zk:Option[ZooKeeper]) = zk.foreach(ZkUtils.close(_))
}
