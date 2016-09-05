package com.manning.fia.c06;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class NewsCountTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {
  private static final long serialVersionUID = 1L;

  private final ValueStateDescriptor<Long> countDescriptor =
    new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

  private final ValueStateDescriptor<Long> timeOutDescriptor =
    new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE, -1L);

  private long maxCount = 0L;
  private long timeOutInMs = 0L;

  private NewsCountTimeoutTrigger(final long maxCount, final long timeOutInMs) {
    this.maxCount = maxCount;
    this.timeOutInMs = timeOutInMs;
  }

  /**
   * Creates a CountTimeout trigger from the given {@code Trigger}.
   * @param maxCount       Maximum Count of Trigger
   * @param sessionTimeout Session Timeout for Trigger to fire
   */
  public static <T, W extends Window> NewsCountTimeoutTrigger<T, W> of(long maxCount, long sessionTimeout) {
    return new NewsCountTimeoutTrigger<>(maxCount, sessionTimeout);
  }

  @Override
  public TriggerResult onElement(T element, long timestamp,
                                 W window, TriggerContext triggerContext) throws Exception {
    final ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);
    final ValueState<Long> timeOutState = triggerContext.getPartitionedState(timeOutDescriptor);

    Long timeSinceLastEvent = 0L;

    // Increment the Count of seen elements each time onElement() is invoked
    final long presentCount = countState.value() + 1;

    // Update the Count State
    countState.update(presentCount);

    if (timeOutState.value() == -1L) {

      long currentTime = triggerContext.getCurrentProcessingTime();

      timeOutState.update(currentTime + timeOutInMs);

      triggerContext.registerProcessingTimeTimer(currentTime + timeOutInMs);

    } else {

      final long lastTimeOutSeen = timeOutState.value();

      timeSinceLastEvent = timestamp - lastTimeOutSeen;

      triggerContext.deleteProcessingTimeTimer(lastTimeOutSeen + timeOutInMs);

      triggerContext.registerProcessingTimeTimer(timestamp + timeOutInMs);

      timeOutState.update(timestamp + timeOutInMs);
    }

    // Fire the trigger if either of maxCount is reached or current Time > timeout value
    if (presentCount >= maxCount || (timestamp >= (timeSinceLastEvent + timeOutInMs))) {
      triggerContext.deleteProcessingTimeTimer(timeOutState.value());
      timeOutState.update(-1L);
      countState.update(0L);
      return TriggerResult.FIRE;
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> timeOutState = triggerContext.getPartitionedState(timeOutDescriptor);
    ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    final Long timeOut = timeOutState.value();
    final Long count = countState.value();

    if (timeOut != -1L) {
      timeOutState.clear();
      triggerContext.deleteProcessingTimeTimer(timeOut);
    }

    if (count != 0) {
      countState.clear();
    }
  }

  @Override
  public String toString() {
    return "NewsCountTimeoutTrigger{" +
      "maxCount=" + maxCount + '}';
  }
}
