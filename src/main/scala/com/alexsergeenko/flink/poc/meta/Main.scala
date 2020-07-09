package com.alexsergeenko.flink.poc.meta

import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.serialization.SerializationSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.extensions._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.util.Collector


case class MetaEvent(key: String, qty: Long)

case class SubjectEvent(key: String, subjectId: Long)

object Main {

  /** main() defines and executes the DataStream program */
  def main(args: Array[String]) {


    // input string: "key->value"
    def extractTupleFromString(s: String, caseType: String) = {
      //      println(s"New incoming string: $s of type: $caseType")
      val strings = s.split("->")
      caseType match {
        case "meta" => MetaEvent(strings.head, strings(1).toLong)
        case "subject" => SubjectEvent(strings.head, strings(1).toLong)
        case _ => println(s"No such type defined: $caseType")
      }
    }

    val conf = new Configuration
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    // set up the streaming execution environment
//    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // checkpoint every 10 seconds
    env.getCheckpointConfig.setCheckpointInterval(10 * 1000) // 10 sec

    // use event time for the application
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // configure watermark interval
    env.getConfig.setAutoWatermarkInterval(1000L)

    // ingest 'meta-events' from socket
    val metaData: DataStream[MetaEvent] = env
      .addSource(new SocketTextStreamFunction("127.0.0.1", 3333, ";", -1))
      .map { s =>
        val strings = s.split("->")
        MetaEvent(strings.head.filter(_ >= ' '), strings(1).toLong)
      }
//    metaData.print("Metadata stream.")


    val subjectData: DataStream[SubjectEvent] = env
      .addSource(new SocketTextStreamFunction("127.0.0.1", 2222, ";", -1))
      .map { s =>
        val strings = s.split("->")
        SubjectEvent(strings.head.filter(_ >= ' '), strings(1).toLong)
      }

//    subjectData.print("Subject stream.")

    subjectData
      .connect(metaData)
      .keyBy("key", "key")
      .process(new MetaEvaluationFunction)
        .writeToSocket("127.0.0.1", 1111, new SerializationSchema[SubjectEvent] {
          override def serialize(element: SubjectEvent): Array[Byte] = {
            element.toString.getBytes()
          }
        })

//        val alerts: DataStream[(String, Double, Double)] = keyedSensorData
//          .flatMap(new TemperatureAlertFunction(1.7))

    /* Scala shortcut to define a stateful FlatMapFunction. */
    //    val alerts: DataStream[(String, Double, Double)] = keyedSensorData
    //      .flatMapWithState[(String, Double, Double), Double] {
    //        case (in: SensorReading, None) =>
    //          // no previous temperature defined. Just update the last temperature
    //          (List.empty, Some(in.temperature))
    //        case (r: SensorReading, lastTemp: Some[Double]) =>
    //          // compare temperature difference with threshold
    //          val tempDiff = (r.temperature - lastTemp.get).abs
    //          if (tempDiff > 1.7) {
    //            // threshold exceeded. Emit an alert and update the last temperature
    //            (List((r.id, r.temperature, tempDiff)), Some(r.temperature))
    //          } else {
    //            // threshold not exceeded. Just update the last temperature
    //            (List.empty, Some(r.temperature))
    //          }
    //      }

    //    // print result stream to standard out
    //    alerts.print()

    // execute application
    env.execute("Matching meta events")
  }
}

/**
 * The function emits an alert if the temperature measurement of a sensor changed by more than
 * a configured threshold compared to the last reading.
 *
 */
//class TemperatureAlertFunction(val threshold: Double)
//  extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
//
//  // the state handle object
//  private var lastTempState: ValueState[Double] = _
//
//  override def open(parameters: Configuration): Unit = {
//    // create state descriptor
//    val lastTempDescriptor = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
//    // obtain the state handle
//    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
//  }
//
//  override def flatMap(reading: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
//
//    // fetch the last temperature from state
//    val lastTemp = lastTempState.value()
//    // check if we need to emit an alert
//    val tempDiff = (reading.temperature - lastTemp).abs
//    if (tempDiff > threshold) {
//      // temperature changed by more than the threshold
//      out.collect((reading.id, reading.temperature, tempDiff))
//    }
//
//    // update lastTemp state
//    this.lastTempState.update(reading.temperature)
//  }
//}

