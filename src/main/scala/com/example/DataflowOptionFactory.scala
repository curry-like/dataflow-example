package com.example

import com.spotify.scio.jdbc.CloudSqlOptions
import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.runners.dataflow.options.DataflowPipelineWorkerPoolOptions.AutoscalingAlgorithmType
import org.apache.beam.sdk.options.PipelineOptionsFactory

object DataflowOptionFactory {

  def createDefaultOption: DataflowOption = {

    val options = PipelineOptionsFactory.as(classOf[DataflowOption])

    options.setProject("")
    options.setRunner(classOf[DataflowRunner])
    options.setZone("us-central1-f")
    options.setNumWorkers(1)
    options.setAutoscalingAlgorithm(AutoscalingAlgorithmType.NONE)
    options.setWorkerMachineType("n1-standard-1")
    options.setCloudSqlInstanceConnectionName("")
    options.setCloudSqlDb("")
    options.setCloudSqlUsername("")
    options.setCloudSqlPassword("")

    options
  }
}

trait DataflowOption extends DataflowPipelineOptions with CloudSqlOptions
