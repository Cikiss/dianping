package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IShopService extends IService<Shop> {
    /**
     * 基于Redis缓存查询
     * @param id
     * @return
     */
    Result queryById(Long id);

    /**
     * 更新商铺信息，同时删除redis中的信息
     * @param shop
     * @return
     */
    Result update(Shop shop);

    /**
     * 根据商铺类型分页查询商铺信息
     * @param typeId
     * @param current
     * @param x
     * @param y
     * @return
     */
    Result queryShopByType(Integer typeId, Integer current, Double x, Double y);
}
