package com.manning.fia.c06;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class SessionWindowExample {

  public static class SessionWindow extends Window {
    private int sessionId;

    public SessionWindow(int sessionId) {
      this.sessionId = sessionId;
    }

    public int getSessionId() {
      return sessionId;
    }

    @Override
    public long maxTimestamp() {
      return Long.MAX_VALUE;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      SessionWindow that = (SessionWindow) o;

      return sessionId == that.sessionId;

    }

    @Override
    public int hashCode() {
      return sessionId;
    }

    public static class Serializer extends TypeSerializer<SessionWindow> {
      private static final long serialVersionUID = 1L;

      @Override
      public boolean isImmutableType() {
        return true;
      }

      @Override
      public TypeSerializer<SessionWindow> duplicate() {
        return this;
      }

      @Override
      public SessionWindow createInstance() {
        return null;
      }

      @Override
      public SessionWindow copy(SessionWindow from) {
        return from;
      }

      @Override
      public SessionWindow copy(SessionWindow from, SessionWindow reuse) {
        return from;
      }

      @Override
      public int getLength() {
        return 0;
      }

      @Override
      public void serialize(SessionWindow record, DataOutputView target) throws IOException {
        target.writeInt(record.getSessionId());
      }

      @Override
      public SessionWindow deserialize(DataInputView source) throws IOException {
        int id = source.readInt();
        return new SessionWindow(id);
      }

      @Override
      public SessionWindow deserialize(SessionWindow reuse, DataInputView source) throws IOException {
        int id = source.readInt();
        return new SessionWindow(id);
      }

      @Override
      public void copy(DataInputView source, DataOutputView target) throws IOException {
        target.writeInt(source.readInt());
      }

      @Override
      public boolean equals(Object obj) {
        return obj instanceof Serializer;
      }

      @Override
      public boolean canEqual(Object obj) {
        return obj instanceof Serializer;
      }

      @Override
      public int hashCode() {
        return 0;
      }
    }

  }

  public static class SessionTrigger extends Trigger<Tuple2<Integer, String>, SessionWindow> {
    private static final long serialVersionUID = 1L;

    private final long sessionTimeout;

    public SessionTrigger(long sessionTimeout) {
      this.sessionTimeout = sessionTimeout;
    }

    @Override
    public TriggerResult onElement(Tuple2<Integer, String> element,
                                   long timestamp,
                                   SessionWindow window,
                                   TriggerContext ctx) throws Exception {
      // this will overwrite previously set timers, so whenever we receive an element
      // this prolongs the final firing time callback
      ctx.registerProcessingTimeTimer(timestamp + sessionTimeout);
      System.out.println(timestamp + " " + (timestamp + sessionTimeout));
      return TriggerResult.CONTINUE;
    }

    @Override
    public TriggerResult onProcessingTime(long time,
                                          SessionWindow window,
                                          TriggerContext ctx) throws Exception {
      return TriggerResult.FIRE_AND_PURGE;
    }

    @Override
    public TriggerResult onEventTime(long time,
                                     SessionWindow window,
                                     TriggerContext ctx) throws Exception {
      return TriggerResult.CONTINUE;
    }
  }

  public static class SessionWindowAssigner extends WindowAssigner<Tuple2<Integer, String>, SessionWindow> {
    private static final long serialVersionUID = 1L;

    @Override
    public Collection<SessionWindow> assignWindows(Tuple2<Integer, String> element,
                                                   long timestamp, WindowAssignerContext windowAssignerContext) {
      return Collections.singleton(new SessionWindow(element.f0));
    }

    @Override
    public Trigger<Tuple2<Integer, String>, SessionWindow> getDefaultTrigger(
      StreamExecutionEnvironment env) {
      return new SessionTrigger(10000);
    }

    @Override
    public TypeSerializer<SessionWindow> getWindowSerializer(ExecutionConfig executionConfig) {
      return new SessionWindow.Serializer();
    }

    @Override
    public boolean isEventTime() {
      return false;
    }
  }


  public static void main(String[] args) throws Exception {

    final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

    DataStream<Tuple2<Integer, String>> input = env.socketTextStream("localhost", 9000)
      .map(new MapFunction<String, Tuple2<Integer, String>>() {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<Integer, String> map(String value) throws Exception {
          String[] splits = value.split(",");
          return Tuple2.of(Integer.parseInt(splits[0]), splits[1]);
        }
      });

    DataStream<String> result = input
      // "group" by the String field
      .keyBy(1)
      // assign to windows based on the first field, the session id field
      .window(new SessionWindowAssigner())
      // trigger after we don't get an element for the key/session combination
      // after 10 seconds
      .trigger(new SessionTrigger(10000))
      .apply(new WindowFunction<Tuple2<Integer,String>, String, Tuple, SessionWindow>() {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(Tuple key,
                          SessionWindow window,
                          Iterable<Tuple2<Integer, String>> values,
                          Collector<String> out) throws Exception {
          int count = 0;
          for (Tuple2<Integer, String> val: values) {
            count++;
          }

          out.collect("We saw " + count +
            " elements in session " + window.getSessionId() +
            " with key " + key);
        }
      });

    result.print();

    env.execute("Session Example");
  }
}
