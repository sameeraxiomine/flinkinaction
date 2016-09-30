package com.manning.fia.c10;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;

public class NewsCountEventTimeTrigger<T, W extends Window> extends Trigger<T, W> {
  private static final long serialVersionUID = 1L;

  private long maxCount = 0L;

  private final ValueStateDescriptor<Long> countDescriptor =
    new ValueStateDescriptor<>("count", LongSerializer.INSTANCE, 0L);

  private NewsCountEventTimeTrigger(long maxCount) {
    this.maxCount = maxCount;
  }

  @Override
  public TriggerResult onElement(T element, long timestamp,
                                 W window, TriggerContext triggerContext) throws Exception {

    // Register event timer for window maxTimestamp
//    triggerContext.registerEventTimeTimer(window.maxTimestamp());

    final ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    // Increment the Count of seen elements each time onElement() is invoked
    countState.update(countState.value() + 1);

    // Fire the trigger early if maxCount is reached
    if (countState.value() >= maxCount || window.maxTimestamp() <= triggerContext.getCurrentWatermark()) {
      countState.update(0L);
      return TriggerResult.FIRE_AND_PURGE;
    } else {
      // Register event timer for window maxTimestamp
      triggerContext.registerEventTimeTimer(window.maxTimestamp());
      return TriggerResult.CONTINUE;
    }
  }

  @Override
  public TriggerResult onProcessingTime(long timestamp, W window,
                                        TriggerContext triggerContext) throws Exception {
    return TriggerResult.CONTINUE;
  }

  @Override
  public TriggerResult onEventTime(long timestamp, W window,
                                   TriggerContext triggerContext) throws Exception {
    return timestamp == window.maxTimestamp() ?
      TriggerResult.FIRE_AND_PURGE :
      TriggerResult.CONTINUE;
  }

  @Override
  public void clear(W window, TriggerContext triggerContext) throws Exception {
    ValueState<Long> countState = triggerContext.getPartitionedState(countDescriptor);

    final Long count = countState.value();
    triggerContext.deleteEventTimeTimer(window.maxTimestamp());

    if (count != 0L) {
      countState.clear();
    }
  }

  /**
   * Creates a CountTimeout trigger from the given {@code Trigger}.
   * @param maxCount Maximum Count of Trigger
   */
  public static <T, W extends Window> NewsCountEventTimeTrigger<T, W> of(long maxCount) {
    return new NewsCountEventTimeTrigger<>(maxCount);
  }

  @Override
  public String toString() {
    return "NewsCountEventTimeTrigger{" +
      "maxCount=" + maxCount + '}';
  }
}
