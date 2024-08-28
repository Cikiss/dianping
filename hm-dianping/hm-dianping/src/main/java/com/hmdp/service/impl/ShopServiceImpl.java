package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import io.netty.util.internal.StringUtil;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;
//    /**
//     * 基于Redis缓存查询
//     * @param id
//     * @return
//     */
//    @Override
//    public Result queryById(Long id) {
//        String cacheShopKey = CACHE_SHOP_KEY + id;
//        // 1.从Redis中查询id
//        String shopJson = stringRedisTemplate.opsForValue().get(cacheShopKey);
//
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 2.命中返回商铺信息
//            return Result.ok(JSONUtil.toBean(shopJson, Shop.class));
//        }
//        // 3.未命中查询数据库
//        Shop shop = getById(id);
//        // 4.商铺不在数据库，返回错误信息
//        if(shop == null){
//            return Result.fail("店铺不存在");
//        }
//        // 5.商铺在数据库
//        // 5.1.保存到Redis中
//        stringRedisTemplate.opsForValue().set(cacheShopKey, JSONUtil.toJsonStr(shop));
//        stringRedisTemplate.expire(cacheShopKey, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        // 5.2.返回商铺信息
//        return Result.ok(shop);
//    }
//    /**
//     * 缓存空值解决缓存穿透
//     * @param id
//     * @return
//     */
//    @Override
//    public Result queryById(Long id) {
//        String cacheShopKey = CACHE_SHOP_KEY + id;
//        // 1.从Redis中查询id
//        String shopJson = stringRedisTemplate.opsForValue().get(cacheShopKey);
//
//        if (StrUtil.isNotBlank(shopJson)) {
//            // 2.命中返回商铺信息
//            return Result.ok(JSONUtil.toBean(shopJson, Shop.class));
//        }else if(shopJson != null){
//            // 2.1.缓存为空直接返回错误信息
//            return Result.fail("店铺信息不存在！");
//        }
//        // 3.未命中查询数据库
//        Shop shop = getById(id);
//        // 4.商铺不在数据库
//        if(shop == null){
//            // 4.1.缓存空值
//            stringRedisTemplate.opsForValue().set(cacheShopKey, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
//            // 4.2.返回错误信息
//            return Result.fail("店铺不存在");
//        }
//        // 5.商铺在数据库
//        // 5.1.保存到Redis中
//        stringRedisTemplate.opsForValue().set(cacheShopKey, JSONUtil.toJsonStr(shop));
//        stringRedisTemplate.expire(cacheShopKey, CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        // 5.2.返回商铺信息
//        return Result.ok(shop);
//    }


    /**
     * 布隆过滤器解决缓存穿透
     * @param id
     * @return
     */
    private BloomFilter bloomFilter;
    @PostConstruct
    public void init(){
        // 初始化布隆过滤器，假设最多有1000000个店铺，误判率为0.01
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8),
                100000, 0.1);
        // 预加载布隆过滤器
        List<Shop> shopList = list();
        for(Shop shop : shopList){
            bloomFilter.put(CACHE_SHOP_KEY + shop.getId());
        }
    }
    private Boolean querySolvingPassThrough(Long id){
        String cacheShopKey = CACHE_SHOP_KEY + id;
        // 1.布隆过滤器过滤，不存在直接返回错误信息
        if(!bloomFilter.mightContain(cacheShopKey)){
            return false;
        }
        return true;
    }

    /**
     * 互斥锁解决缓存击穿
     */
    private Boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", LOCK_SHOP_TTL, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }
    public Shop queryWithMutex(Long id){
        // 2.可能存在，从Redis中查询id
        String cacheShopKey = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(cacheShopKey);
        if (StrUtil.isNotBlank(shopJson)) {
            // 2.1命中返回商铺信息
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        Shop shop = null;
        String lockKey = LOCK_SHOP_KEY + id;
        Boolean isLock = tryLock(lockKey);
        try {
            // 3.未命中，尝试获取锁
            if (!isLock) {
                // 3.1.未获取到锁，休眠递归
                Thread.sleep(50);
                return queryWithMutex(id);
            }
            // 3.2.1.获取到锁，根据id查数据库
            shop = getById(id);
            Thread.sleep(200);
            // 3.2.2.商铺不在数据库，返回错误信息
            if(shop == null){
                return null;
            }
            // 3.2.3.商铺在数据库，写入Redis
            stringRedisTemplate.opsForValue().set(cacheShopKey, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 4.释放互斥锁
            unlock(lockKey);
        }
        // 5.返回数据
        return shop;
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
    /**
     * 提前预热
     * @param id
     */
    public void saveShopToRedis(Long id, Long expireSeconds) throws Exception {
        // 1.查询店铺数据
        Shop shop = getById(id);
        // 模拟延迟
        Thread.sleep(200);
        // 2.封装逻辑过期时间
        RedisData redisData = new RedisData(LocalDateTime.now().plusSeconds(expireSeconds), shop);
        // 3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }
    /**
     * 逻辑过期解决缓存击穿
     * @param id
     * @return
     */
    public Shop queryWithLogicalExpire(Long id){
        String cacheShopKey = CACHE_SHOP_KEY + id;
        // 1.查询Redis
        String redisDataJson = stringRedisTemplate.opsForValue().get(cacheShopKey);
        // 2.未命中返回空
        if (StrUtil.isBlank(redisDataJson)) {
            return null;
        }
        // 3.命中，判断缓存是否过期
        RedisData redisData = JSONUtil.toBean(redisDataJson, RedisData.class);
        // redisData中的data是JSONObject而不是Object
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        // 4.未过期，返回商铺信息
        if(LocalDateTime.now().isAfter(redisData.getExpireTime())){
            // 5.过期，尝试获取互斥锁
            String lockKey = LOCK_SHOP_KEY + id;
            Boolean isLock = tryLock(lockKey);
            // 5.1.获取锁失败，跳过
            if(isLock){
                // 5.2.获取锁成功，开启独立线程
                CACHE_REBUILD_EXECUTOR.submit(() -> {
                    // 5.2.1.缓存重建
                    try {
                        this.saveShopToRedis(id, 20L);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        // 5.2.2.释放锁
                        unlock(lockKey);
                    }
                });
            }
        }
        // 5.3.返回旧商铺信息
        return shop;
    }


    /**
     * 根据id查询店铺信息
     * @param id
     * @return
     */
    @Override
    public Result queryById(Long id) {

        // 1.1.缓存空值解决缓存穿透
//        Shop shop = cacheClient.
//                querySolvingPassThroughByNull(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 1.2.布隆过滤器解决缓存穿透
        if (!querySolvingPassThrough(id)) {
            return Result.fail("商铺不存在！");
        }

        // 2.1.互斥锁解决缓存击穿
        /*// 2.1.1.未使用封装工具
        Shop shop = queryWithMutex(id);*/
        // 2.1.2.使用封装工具
        Shop shop = cacheClient
                .queryWithMutex(CACHE_SHOP_KEY, LOCK_SHOP_KEY, id, Shop.class, this::getById,
                        CACHE_SHOP_TTL, TimeUnit.MINUTES, LOCK_SHOP_TTL, TimeUnit.SECONDS);

        // 2.2.逻辑过期解决缓存击穿
        /*// 2.2.1.未使用封装工具
        Shop shop = queryWithLogicalExpire(id);*/
        /*// 2.2.2.使用封装工具
        Shop shop = cacheClient.
                queryWithLogicalExpire(CACHE_SHOP_KEY, LOCK_SHOP_KEY, id, Shop.class, this::getById,
                        CACHE_SHOP_TTL, TimeUnit.MINUTES, LOCK_SHOP_TTL, TimeUnit.SECONDS);*/
        if (shop == null) {
            return Result.fail("商铺不存在！");
        }
        return Result.ok(shop);
    }

    /**
     * 更新商铺信息，同时删除redis中的信息
     * @param shop
     * @return
     */
    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除redis中的信息
        String key = RedisConstants.CACHE_SHOP_KEY + id;
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    /**
     * 根据商铺类型分页查询商铺信息
     * @param typeId
     * @param current
     * @param x
     * @param y
     * @return
     */
    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if(x == null || y == null){
            // 不需要坐标查询，查询数据库
            Page<Shop> page = query().eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            return Result.ok(page.getRecords());
        }
        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        // 3.查询Redis，按照距离排序、分页。结果为：shopId, distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs
                                .newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if(results == null){
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if(list.size() <= from){
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取从from到end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<Long, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            Long shopId = Long.valueOf(result.getContent().getName());
            ids.add(shopId);
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopId, distance);
        });
        // 5.根据id查询shop
        CharSequence idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD (id, " + idStr + ")").list()
                .stream().map(shop -> shop.setDistance(distanceMap.get(shop.getId()).getValue()))
                .collect(Collectors.toList());
        // 6.返回
        return Result.ok(shops);
    }
}
