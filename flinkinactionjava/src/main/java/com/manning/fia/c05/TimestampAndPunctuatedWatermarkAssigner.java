package com.manning.fia.c05;

import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.joda.time.format.DateTimeFormat;

import com.manning.fia.model.media.NewsFeed;
import com.manning.fia.model.media.WaterMarkedNewsFeed;

public class TimestampAndPunctuatedWatermarkAssigner implements AssignerWithPunctuatedWatermarks<NewsFeed> {
	@Override
	public Watermark checkAndGetNextWatermark(NewsFeed newsFeed, long extractedTs) {
		return newsFeed instanceof WaterMarkedNewsFeed ? new Watermark(extractedTs) : null;
	}

	@Override
	public long extractTimestamp(NewsFeed newsFeed, long l) {
		long millis = DateTimeFormat.forPattern("yyyyMMddHHmmss").parseDateTime(newsFeed.getStartTimeStamp()).getMillis();
		return millis;
	}
}