package services.util

import java.util

/**
 * Created by king on 15-9-21.
 */
class LRUCache[K, V](cacheSize: Int, loadingFactor: Float = 0.75f) {
  private val cache = {
    val initialCapacity = math.ceil(cacheSize / loadingFactor).toInt + 1
    new util.LinkedHashMap[K, V](initialCapacity, loadingFactor, true)
  }

  private var cacheMiss = 0
  private var cacheHit = 0

  def size: Int = cache.size()

  def containsKey(k: K): Boolean = cache.containsKey(k)

  def get(k: K, v: => V): V = {
    cache.get(k) match {
      case null =>
        val evaluatedV = v
        cache.put(k, evaluatedV)
        cacheMiss += 1
        evaluatedV
      case vv =>
        cacheHit += 1
        vv
    }
  }

  def cacheHitRatio: Double = cacheHit.toDouble / math.max(cacheHit + cacheMiss, 1)

  def put(k: K, v: V): V = cache.put(k, v)

  def get(k: K): Option[V] = Option(cache.get(k))
}
