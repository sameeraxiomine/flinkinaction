package com.manning.fia.c06;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class NewsTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {

  private Trigger<T, W> nestedTrigger;
  private final long sessionTimeout;

  private final ValueStateDescriptor<Long> stateDesc =
    new ValueStateDescriptor<>("start-time", LongSerializer.INSTANCE, null);

  public NewsTimeoutTrigger(Trigger<T, W> nestedTrigger,
                            final Long sessionTimeout) {
    this.sessionTimeout = sessionTimeout;
    this.nestedTrigger = nestedTrigger;
  }

  @Override
  public TriggerResult onElement(T newsFeed, long timestamp, W window,
                                 TriggerContext triggerContext) throws Exception {
//    ValueState<Long> startTimeState = triggerContext.getPartitionedState(stateDesc);
//    if (startTimeState.value() == null) {
//      long currentTime = triggerContext.getCurrentProcessingTime();
//      startTimeState.update(currentTime);
//      triggerContext.registerProcessingTimeTimer(currentTime + sessionTimeout);
//    }
    TriggerResult triggerResult = nestedTrigger.onElement(newsFeed, timestamp, window, triggerContext);
    return triggerResult.isFire() ? TriggerResult.FIRE_AND_PURGE : triggerResult;
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
    System.out.println(window.maxTimestamp());
    ValueState<Long> startTimeState = triggerContext.getPartitionedState(stateDesc);
    if (startTimeState.value() == null) {
      long currentTime = window.maxTimestamp();
      startTimeState.update(currentTime);
      triggerContext.registerProcessingTimeTimer(currentTime + sessionTimeout);
    }
    System.out.println(window.maxTimestamp());
    return TriggerResult.FIRE_AND_PURGE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> startTimeState = triggerContext.getPartitionedState(stateDesc);
    Long startTime = startTimeState.value();
    if (startTime != null) {
      startTimeState.clear();
      triggerContext.deleteProcessingTimeTimer(startTime + sessionTimeout);
    }
    nestedTrigger.clear(window, triggerContext);
  }

  /**
   * Creates a new purging trigger from the given {@code Trigger}.
   *
   * @param nestedTrigger The trigger that is wrapped by this purging trigger
   */
  public static <T, W extends Window> NewsTimeoutTrigger<T, W> of(Trigger<T, W> nestedTrigger, long sessionTimeout) {
    return new NewsTimeoutTrigger<>(nestedTrigger, sessionTimeout);
  }

  @Override
  public String toString() {
    return "NewsTimeoutTrigger()";
  }

  private static class Sum implements ReduceFunction<Long> {
    private static final long serialVersionUID = 1L;

    @Override
    public Long reduce(Long value1, Long value2) throws Exception {
      return value1 + value2;
    }

  }
}
