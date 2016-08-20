package com.manning.fia.ch06;

import com.manning.fia.model.media.NewsFeed;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

public class NewsCountTrigger extends Trigger<NewsFeed, GlobalWindow> {

  private int numberOfElementsToFireFor;

  public NewsCountTrigger(int numberOfElementsToFireFor) {
    this.numberOfElementsToFireFor = numberOfElementsToFireFor;
  }

  @Override
  public TriggerResult onElement(NewsFeed newsFeed, long l, GlobalWindow window,
                                 TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long l, GlobalWindow window,
                                        TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long l, GlobalWindow window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

}
