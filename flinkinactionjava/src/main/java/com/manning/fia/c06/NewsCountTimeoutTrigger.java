package com.manning.fia.c06;

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class NewsCountTimeoutTrigger<T, W extends Window> extends Trigger<T, W> {
  private static final long serialVersionUID = 1L;

  private long maxCount = 0L;
  private long timeOutInMs = 0L;

  private final ValueStateDescriptor<Long> countDescriptor =
    new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

  private final ValueStateDescriptor<Long> timeOutDescriptor =
    new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE, null);

  private NewsCountTimeoutTrigger(long maxCount, long timeOutInMs) {
    this.maxCount = maxCount;
    this.timeOutInMs = timeOutInMs;
  }

  @Override
  public TriggerResult onElement(T element, long timestamp,
                                 W window, TriggerContext triggerContext) throws Exception {
    final ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);
    final ValueState<Long> timeOutState = triggerContext.getPartitionedState(timeOutDescriptor);

    // Get the Current Timestamp
    final long currentTimeStamp = System.currentTimeMillis();

    // Increment the Count each time onElement() is invoked
    final long presentCount = countState.value() + 1;

    if (timeOutState.value() == null) {
      long currentTime = triggerContext.getCurrentProcessingTime();
      timeOutState.update(currentTime);
      triggerContext.registerProcessingTimeTimer(currentTime + timeOutInMs);
    }

    // Get the time out value
    final long timeoutValue = timeOutState.value();

    if (currentTimeStamp > timeoutValue || presentCount >= maxCount) {
      return process(timeOutState, countState);
    }

    countState.update(presentCount);

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
    final ValueState<Long> timeOut = triggerContext.getPartitionedState(timeOutDescriptor);
    final ValueState<Long> count = triggerContext.getPartitionedState(countDescriptor);
    if (timeOut.value() == timestamp) {
      return process(timeOut, count);
    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> timeOutState = triggerContext.getPartitionedState(timeOutDescriptor);
    final Long timeOut = timeOutState.value();

    if (timeOut != null) {
      timeOutState.clear();
      triggerContext.deleteProcessingTimeTimer(timeOut);
    }

    timeOutState.clear();
  }

  /**
   *
   * @param timeOut - The present TimeOut Value
   * @param count - The present Count
   * @return {@code TriggerResult}
   * @throws IOException
   */
  private TriggerResult process(ValueState<Long> timeOut, ValueState<Long> count) throws IOException {
    timeOut.update(null);
    count.update(0L);
    return TriggerResult.FIRE;
  }

  /**
   * Creates a CountTimeout trigger from the given {@code Trigger}.
   * @param maxCount Maximum Count of Trigger
   * @param sessionTimeout Session Timeout for Trigger to fire
   */
  public static <T, W extends Window> NewsCountTimeoutTrigger<T, W> of(long maxCount, long sessionTimeout) {
    return new NewsCountTimeoutTrigger<>(maxCount, sessionTimeout);
  }

  @Override
  public String toString() {
    return "NewsCountTimeoutTrigger{" +
      "maxCount=" + maxCount + '}';
  }
}
