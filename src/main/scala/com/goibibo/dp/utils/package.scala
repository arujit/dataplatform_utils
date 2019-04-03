package com.goibibo.dp.utils

object common {

		def getAsOption[T](v:Any):Option[T] = {
			if(v != null) Some(v.asInstanceOf[T])
			else None
		}

		def getAsOptionT[T](v:T):Option[T] = {
			if(v != null) Some(v.asInstanceOf[T])
			else None
		}
	}