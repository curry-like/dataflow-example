package com.example

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{ CreateDisposition, WriteDisposition }

object WordCount {

  @BigQueryType.toTable
  case class Output(
    word: String,
    count: Long)

  def main(args: Array[String]): Unit = {

    val option = DataflowOptionFactory.createDefaultOption
    val inputPath = ""
    val outputTable = ""
    val sc = ScioContext(option)

    sc.textFile(inputPath)
      .flatMap(_.split("[^a-zA-Z']+").filter(_.nonEmpty))
      .countByValue
      .map {
        case (word, count) =>
          Output(word = word, count = count)
      }
      .saveAsTypedBigQuery(outputTable, WriteDisposition.WRITE_APPEND, CreateDisposition.CREATE_IF_NEEDED)

    sc.close().waitUntilFinish()
  }
}
