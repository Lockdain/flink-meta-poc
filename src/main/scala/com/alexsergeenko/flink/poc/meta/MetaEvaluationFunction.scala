package com.alexsergeenko.flink.poc.meta

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction
import org.apache.flink.util.Collector

class MetaEvaluationFunction extends KeyedCoProcessFunction[String, SubjectEvent, MetaEvent, SubjectEvent] {
  implicit val typeInfo = TypeInformation.of(classOf[Long])
  lazy val subjectEventsCount: ValueState[Long] =
    getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("subjectEventsCount", typeInfo))
  lazy val metaEventCount: ValueState[Long] =
    getRuntimeContext
      .getState(new ValueStateDescriptor[Long]("metaEventsCount", typeInfo))
  lazy val subjectEventsWaitingList: ListState[SubjectEvent] =
    getRuntimeContext
      .getListState(new ListStateDescriptor[SubjectEvent]("eventsAggregation", TypeInformation.of(classOf[SubjectEvent])))

  override def processElement1(value: SubjectEvent, ctx: KeyedCoProcessFunction[String, SubjectEvent, MetaEvent, SubjectEvent]#Context, out: Collector[SubjectEvent]): Unit = {
    println(s"New subject event: $value")
    println(s"Current key is: ${ctx.getCurrentKey}")
    val oldCount = subjectEventsCount.value();
    var newCount = oldCount + 1;
    println(s"The new count of subject events is: $newCount")
    println(s"Waiting for match in meta: ${metaEventCount.value()}")
    subjectEventsCount.update(newCount)
    subjectEventsWaitingList.add(value)
    println("New subject event was added to the waiting list.")
    if (metaEventCount.value() == subjectEventsCount.value()) {
      println("Meta events qty match!")
      subjectEventsWaitingList.get().forEach(event => out.collect(event))
      subjectEventsWaitingList.clear()
    }
  }

  override def processElement2(value: MetaEvent, ctx: KeyedCoProcessFunction[String, SubjectEvent, MetaEvent, SubjectEvent]#Context, out: Collector[SubjectEvent]): Unit = {
    println(s"Current key is: ${ctx.getCurrentKey}")
    println(s"New meta event: $value")
    metaEventCount.update(value.qty)
    println(s"Meta event qty now is: ${metaEventCount.value()}")
    if (metaEventCount.value() == subjectEventsCount.value()) {
      println("Meta events qty match!")
      subjectEventsWaitingList.get().forEach(event => out.collect(event))
      subjectEventsWaitingList.clear()
    }
  }
}
