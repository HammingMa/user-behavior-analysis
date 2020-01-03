package com.mzh.flink.hot_items_analysis.app.model

case class ItemViewCount (itemId: Long, windowEnd: Long, count: Long)
