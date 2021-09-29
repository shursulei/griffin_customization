/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.griffin.measure.launch.batch

import java.util.concurrent.TimeUnit

import scala.util.Try

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.griffin.measure.configuration.dqdefinition._
import org.apache.griffin.measure.configuration.enums.ProcessType.BatchProcessType
import org.apache.griffin.measure.context._
import org.apache.griffin.measure.datasource.DataSourceFactory
import org.apache.griffin.measure.job.builder.DQJobBuilder
import org.apache.griffin.measure.launch.DQApp
import org.apache.griffin.measure.step.builder.udf.GriffinUDFAgent
import org.apache.griffin.measure.utils.CommonUtils

case class BatchDQApp(allParam: GriffinConfig) extends DQApp {

  val envParam: EnvConfig = allParam.getEnvConfig
  val dqParam: DQConfig = allParam.getDqConfig

  val sparkParam: SparkParam = envParam.getSparkParam
  val metricName: String = dqParam.getName
  val sinkParams: Seq[SinkParam] = getSinkParams

  var dqContext: DQContext = _

  def retryable: Boolean = false
  // 初始化并创建sparkSession、注册griffin自定义udf
  def init: Try[_] = Try {
    // build spark 2.0+ application context
    val conf = new SparkConf().setAppName(metricName)
    conf.setAll(sparkParam.getConfig)
    conf.set("spark.sql.crossJoin.enabled", "true")
    sparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val logLevel = getGriffinLogLevel
    sparkSession.sparkContext.setLogLevel(sparkParam.getLogLevel)
    griffinLogger.setLevel(logLevel)

    // register udf
    GriffinUDFAgent.register(sparkSession)
  }
  // 定时任务执行方法
  def run: Try[Boolean] = {
    val result = CommonUtils.timeThis({
      val measureTime = getMeasureTime
      val contextId = ContextId(measureTime)
      // 根据配置获取数据源，即config-batch.json的data.sources配置，读取avro文件数据，有source和target两个数据源
      // get data sources
      val dataSources =
        DataSourceFactory.getDataSources(sparkSession, null, dqParam.getDataSources)
      // 数据源初始化
      dataSources.foreach(_.init())
      // 创建griffin执行上下文
      // create dq context
      dqContext =
        DQContext(contextId, metricName, dataSources, sinkParams, BatchProcessType)(sparkSession)
      // 根据配置，输入结果到 console 和 elasticsearch
      // start id
      val applicationId = sparkSession.sparkContext.applicationId
      dqContext.getSinks.foreach(_.open(applicationId))
      // 创建数据检查对比job
      // build job
      val dqJob = DQJobBuilder.buildDQJob(dqContext, dqParam.getEvaluateRule)
      // 执行数据对比job，根据在web端配置的步骤执行，demo主要执行配置中的rule sql，将执行结果写入sink中
      // dq job execute
      dqJob.execute(dqContext)
    }, TimeUnit.MILLISECONDS)

    // clean context
    dqContext.clean()
    // 输出结束标记
    // finish
    dqContext.getSinks.foreach(_.close())

    result
  }

  def close: Try[_] = Try {
    sparkSession.stop()
  }

}
