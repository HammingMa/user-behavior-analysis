package com.mzh.flink.hot_items_analysis.app.model

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
