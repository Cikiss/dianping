package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 开始时间戳
     * public static void main(String[] args) {
     *         LocalDateTime beginTime = LocalDateTime.of(2024, 1, 1, 0, 0);
     *         long second = beginTime.toEpochSecond(ZoneOffset.UTC);
     *         System.out.println(second);
     *     }
     */
    private static final long BEGIN_TIMESTAMP = 1704067200;

    /**
     * 序列号位数
     */
    private static final long BITS_COUNT = 32;


    /**
     * 生成全局唯一id
     * @param keyPrefix
     * @return
     */
    public long nextId(String keyPrefix){
        // 1.生成时间戳
        LocalDateTime nowTime = LocalDateTime.now();
        long nowSecond = nowTime.toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowSecond - BEGIN_TIMESTAMP;
        // 2.生成序列号
        // 2.1.获取当前日期，精确到秒 (:是为了方便Redis使用)
        String date = nowTime.format(DateTimeFormatter.ofPattern("yyyy:MM:dd:HH:mm:ss"));
        // 2.2.自增长
        Long count = stringRedisTemplate.opsForValue().increment("icr:" + keyPrefix + ":" + date);
        // 3.拼接并返回
        return (timestamp << BITS_COUNT) | count;
    }
}
