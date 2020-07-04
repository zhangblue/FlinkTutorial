package com.zhangblue.entity

/**
 * 温度传感器对象类
 *
 * @param id          传感器id
 * @param timestamp   时间戳
 * @param name        旅客的名字
 * @param temperature 温度
 * @param location    传感器位置
 */
case class TemperatureSensor(id: String, timestamp: Long, name: String, temperature: Double, location: String)