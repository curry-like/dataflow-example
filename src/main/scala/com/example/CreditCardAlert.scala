package com.example

import java.sql.Driver

import com.spotify.scio.ScioContext
import com.spotify.scio.jdbc.{JdbcConnectionOptions, _}
import io.circe.generic.auto._
import io.circe.parser.decode
import org.joda.time.{DateTime, Duration}

object CreditCardAlert {

  private[this] case class CreditUse(
                        userId: Long,
                      amount: Long,
                      DateTime: DateTime,
                      shopId: Long)

  private[this] case class Shop(
                               shopId: Long,
                               location: String
                               )

  def main(args: Array[String]): Unit = {

    val option = DataflowOptionFactory.createDefaultOption
    val sc = ScioContext(option)
    val inputTopic = ""
    val outputTopic = ""

    val jdbcUrl = s"jdbc:mysql://google/${option.getCloudSqlDb}?" +
      s"cloudSqlInstance=${option.getCloudSqlInstanceConnectionName}&" +
      s"socketFactory=com.google.cloud.sql.mysql.SocketFactory"

    val connOption = JdbcConnectionOptions(
      username = option.getCloudSqlUsername,
      password = Option(option.getCloudSqlPassword),
      driverClass = classOf[Driver],
      connectionUrl = jdbcUrl)

    val shops = sc.jdbcSelect(getShops()(connOption))
      .keyBy(_.shopId)

    sc.pubsubTopic[String]("")
      .withFixedWindows(Duration.standardMinutes(10))
      .map {json =>
        decode[CreditUse](json).right.get
      }
      .keyBy(_.shopId)
      .join(shops)
      .values
      .groupBy(_._1.userId)
      .values
      .filter(_.size > 2)
      .filter(_.exists(_._2.location != "Japan"))
      .map(_.head._1)
      .saveAsPubsub(outputTopic)

    sc.close()
  }

  def getShops()(connOptions: JdbcConnectionOptions): JdbcReadOptions[Shop] = ???
}
