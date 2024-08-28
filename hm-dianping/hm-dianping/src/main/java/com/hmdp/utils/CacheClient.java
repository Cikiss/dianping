package com.hmdp.utils;


import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.*;

@Component
@Slf4j
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {

        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 存入Redis，并设置过期时间
    public void set(String key, Object value, Long time, TimeUnit unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    // 存入Redis，并设置逻辑过期时间
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit){
        // 设置逻辑过期时间
        RedisData redisData = new RedisData(LocalDateTime.now().plusSeconds(unit.toSeconds(time)), value);
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 缓存空值，解决缓存穿透
    public <R, ID> R querySolvingPassThroughByNull(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String cacheKey = keyPrefix + id;
        // 1.从Redis中查询id
        String json = stringRedisTemplate.opsForValue().get(cacheKey);

        if (StrUtil.isNotBlank(json)) {
            // 2.命中返回商铺信息
            return JSONUtil.toBean(json, type);
        }else if(json != null){
            // 2.1.缓存为空直接返回空
            return null;
        }
        // 3.未命中查询数据库
        R r = dbFallback.apply(id);
        // 4.商铺不在数据库
        if(r == null){
            // 4.1.缓存空值
            stringRedisTemplate.opsForValue().set(cacheKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 4.2.返回错误信息
            return null;
        }
        // 5.商铺在数据库
        // 5.1.保存到Redis中
        this.set(cacheKey, r, time, unit);
        // 5.2.返回商铺信息
        return r;
    }

    // 逻辑过期，解决缓存击穿
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    private Boolean tryLock(String key, Long time, TimeUnit unit){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", time, unit);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, String lockPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
            Long cacheTime, TimeUnit cacheUnit, Long lockTime, TimeUnit lockUnit){
        String cacheKey = keyPrefix + id;
        // 1.查询Redis
        String redisDataJson = stringRedisTemplate.opsForValue().get(cacheKey);
        // 2.未命中返回空
        if (StrUtil.isBlank(redisDataJson)) {
            return null;
        }
        // 3.命中，判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(redisDataJson, RedisData.class);
        // redisData中的data是JSONObject而不是Object
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        // 4.未过期，返回商铺信息
        if(LocalDateTime.now().isAfter(redisData.getExpireTime())) {
            // 5.过期，尝试获取互斥锁
            String lockKey = lockPrefix + id;
            Boolean isLock = tryLock(lockKey, lockTime, lockUnit);
            // 5.1.获取锁失败，跳过
            if (isLock) {
                // 5.2.获取锁成功，开启独立线程
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 5.2.1.缓存重建
                    try {
                        // 查询数据库
                        R r1 = dbFallback.apply(id);
                        // 写入Redis
                        this.setWithLogicalExpire(cacheKey, r1, cacheTime, cacheUnit);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 5.2.2.释放锁
                        unlock(lockKey);
                    }
                });
            }
        }
        return r;
    }

    // 互斥锁，解决缓存击穿
    public <R, ID> R queryWithMutex(
            String keyPrefix, String lockPrefix, ID id, Class<R> type, Function<ID, R> dbFallback,
            Long cacheTime, TimeUnit cacheUnit, Long lockTime, TimeUnit lockUnit){
        // 2.可能存在，从Redis中查询id
        String cacheKey = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(cacheKey);
        if (StrUtil.isNotBlank(json)) {
            // 2.1命中返回商铺信息
            return JSONUtil.toBean(json, type);
        }
        R r = null;
        String lockKey = lockPrefix + id;
        Boolean isLock = tryLock(lockKey, lockTime, lockUnit);
        try {
            // 3.未命中，尝试获取锁
            if (!isLock) {
                // 3.1.未获取到锁，休眠递归
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, lockPrefix, id, type, dbFallback, cacheTime, cacheUnit, lockTime, lockUnit);
            }
            // 3.2.1.获取到锁，根据id查数据库
            r = dbFallback.apply(id);
            Thread.sleep(200);
            // 3.2.2.商铺不在数据库，返回错误信息
            if(r == null){
                return null;
            }
            // 3.2.3.商铺在数据库，写入Redis
            this.set(cacheKey, r, cacheTime, cacheUnit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 4.释放互斥锁
            unlock(lockKey);
        }
        // 5.返回数据
        return r;
    }
}
