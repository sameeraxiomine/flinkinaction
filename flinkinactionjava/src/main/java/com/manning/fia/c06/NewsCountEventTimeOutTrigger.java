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

  private final ValueStateDescriptor<Long> countDescriptor =
    new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

  private NewsCountEventTimeOutTrigger(long maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public TriggerResult onElement(T element, long timestamp,
                                 W window, TriggerContext triggerContext) throws Exception {
    final ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    // Increment the Count of seen elements each time onElement() is invoked
    final long presentCount = countState.value() + 1;

    // Fire the trigger if either of maxCount is reached or watermark has passed the Window's maxTimeStamp
    if (presentCount >= maxCount || window.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
      return process(countState);
    }

    countState.update(presentCount);

    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return TriggerResult.FIRE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    final Long count = countState.value();
    triggerContext.deleteEventTimeTimer(window.maxTimestamp());

    if (count != null) {
      countState.clear();
    }
  }

  /**
   *
   * @param count - The present Count
   * @return {@code TriggerResult}
   * @throws IOException
   */
  private TriggerResult process(ValueState<Long> count) throws IOException {
    count.update(0L);
    return TriggerResult.FIRE;
  }

  /**
   * Creates a CountTimeout trigger from the given {@code Trigger}.
   * @param maxCount Maximum Count of Trigger
   */
  public static <T, W extends Window> NewsCountEventTimeOutTrigger<T, W> of(long maxCount) {
    return new NewsCountEventTimeOutTrigger<>(maxCount);
  }

  @Override
  public String toString() {
    return "NewsCountEventTimeOutTrigger{" +
      "maxCount=" + maxCount + '}';
  }
}
