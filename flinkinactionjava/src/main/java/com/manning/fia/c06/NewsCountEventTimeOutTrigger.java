package com.manning.fia.c06;

import java.io.IOException;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class NewsCountEventTimeOutTrigger<T, W extends Window> extends Trigger<T, W> {
  private static final long serialVersionUID = 1L;

  private long maxCount = 0L;
  private long timeOutInMs = 0L;

  private final ValueStateDescriptor<Long> countDescriptor =
    new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

  private final ValueStateDescriptor<Long> timeOutDescriptor =
    new ValueStateDescriptor<>("timeout", LongSerializer.INSTANCE, null);

  private NewsCountEventTimeOutTrigger(long maxCount, long timeOutInMs) {
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

    if (timeOutState.value() == null) {
      timeOutState.update(timestamp);
      triggerContext.registerEventTimeTimer(timestamp + timeOutInMs);
    }

    // Increment the Count of seen elements each time onElement() is invoked
    final long presentCount = countState.value() + 1;

    // Get the time out value
    final long timeoutValue = timeOutState.value();

    // Fire the trigger if either of maxCount is reached or current Time > timeout value
    if (presentCount >= maxCount || currentTimeStamp > timeoutValue) {
      return process(timeOutState, countState);
    }

    countState.update(presentCount);

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
//    final ValueState<Long> timeOut = triggerContext.getPartitionedState(timeOutDescriptor);
//    final ValueState<Long> count = triggerContext.getPartitionedState(countDescriptor);
//    if (timeOut.value() == timestamp) {
//      return process(timeOut, count);
//    }

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> timeOutState = triggerContext.getPartitionedState(timeOutDescriptor);
    ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    final Long timeOut = timeOutState.value();
    final Long count = countState.value();

    if (timeOut != null) {
      timeOutState.clear();
      triggerContext.deleteEventTimeTimer(timeOut);
    }

    if (count != null) {
      countState.clear();
    }
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
  public static <T, W extends Window> NewsCountEventTimeOutTrigger<T, W> of(long maxCount, long sessionTimeout) {
    return new NewsCountEventTimeOutTrigger<>(maxCount, sessionTimeout);
  }

  @Override
  public String toString() {
    return "NewsCountEventTimeOutTrigger{" +
      "maxCount=" + maxCount + '}';
  }
}
