package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.conditions.query.QueryChainWrapper;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 基于Redis查询商铺类型
     * @return
     */
    @Override
    public List<ShopType> queryWithRedis() {
        // 1.如果Redis中存在，直接返回
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_TYPE_KEY);
        // 2.存在，直接返回
        if(StrUtil.isNotBlank(shopJson)){
            List<ShopType> list = JSONUtil.toList(shopJson, ShopType.class);
            return list;
        }
        // 3.不存在
        // 3.1.查询数据库
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        // 3.2.保存到Redis中
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_TYPE_KEY, JSONUtil.toJsonStr(shopTypes));
        // 3.3.返回商铺类型信息
        return shopTypes;
    }
}
