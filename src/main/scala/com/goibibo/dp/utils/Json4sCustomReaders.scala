package com.goibibo.dp.utils

import java.sql.Timestamp
import java.util.Date

import org.joda.time.{DateTime, Days}
import org.json4s._

import scala.util.Try

class CustomTimestampSerializer extends CustomSerializer[Timestamp](format => ( {
    case JInt(x) => new Timestamp(x.longValue)
    case JNull => null
}, {
    case date: Timestamp => JInt(date.getTime)
})
)

class CustomDateSerializer extends CustomSerializer[Date](format => ( {
    case JInt(x) =>
        Constants.EPOCH_START_INSTANT.plusDays(x.intValue).toDate
    case JNull => null
}, {
    case date: Date =>
        JInt(Days.daysBetween(Constants.EPOCH_START_INSTANT, new DateTime(date)).getDays)
})
)

class CustomBoolSerializer extends CustomSerializer[Boolean](format => ( {
    case JInt(x) =>
        if (x == 0) false else true
    case JString(x) =>
        if (x.toLowerCase == "t" || x.toLowerCase == "true") true else false
}, {
    case x: Boolean =>
        JBool(x)
})
)

class CustomIntSerializer extends CustomSerializer[Int](format => ( {
    case JString(x) =>
        Try(x.toInt).getOrElse(0)
}, {
    case x: Int =>
        JInt(x)
})
)

class CustomDoubleSerializer extends CustomSerializer[Double](format => ( {
    case JString(x) =>
        Try(x.toDouble).getOrElse(0.0)
}, {
    case x: Double =>
        JDouble(x)
})
)


object StringToDate extends CustomSerializer[java.util.Date](format => ( {
    case JString(x) =>
        val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S")
        Try {
            fmt.parse(x)
        }.getOrElse(null)
}, {
    case x: java.util.Date =>
        val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S")
        JString(fmt.format(x))
}
))

object StringToTimestamp extends CustomSerializer[java.sql.Timestamp](format => ( {
    case JString(x) =>
        val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S")
        new java.sql.Timestamp(fmt.parse(x).getTime)
}, {
    case x: java.sql.Timestamp =>
        val fmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss.S")
        JString(fmt.format(x))
}
))

object Constants {
    val EPOCH_START_INSTANT: DateTime = new DateTime(0)
}
