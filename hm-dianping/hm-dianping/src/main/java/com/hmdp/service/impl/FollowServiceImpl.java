package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private IUserService userService;
    /**
     * 关注或取关
     * @param followUserId
     * @param isFollow true：关注， false：取关
     * @return
     */
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        // 1.得到当前用户id
        Long userId = UserHolder.getUser().getId();
        if(userId == null) {
            return Result.fail("未登录！");
        }
        // Redis中set的key
        String key = "follows:" + userId;
        // 判断关注或取关
        if(isFollow){
            // 2.关注，新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if(isSuccess){
                // 把关注用户的id，放入Redis的set中 sadd key followedUserId
                stringRedisTemplate.opsForSet().add(key, followUserId.toString());
            }
        }else{
            // 3.取关，删除数据
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if(isSuccess){
                // 把关注用户的id，在Redis的set中移除 srem key followedUserId
                stringRedisTemplate.opsForSet().remove(key, followUserId.toString());
            }
        }
        return Result.ok();
    }

    /**
     * 判断是否关注
     * @param followUserId
     * @return
     */
    @Override
    public Result isFollow(Long followUserId) {
        // 1.获取当前用户id
        Long userId = UserHolder.getUser().getId();
        // 2.查询是否关注
        // 2.1.先查Redis中的set
        String key = "follows:" + userId;
        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, followUserId.toString());
        if(isMember){
            return Result.ok(true);
        }
        // 2.2.后查DB
        Integer count = query().eq("user_id", userId).eq("follow_user_id", followUserId).count();
        // 3.判断
        boolean isFollow = count != null && count > 0;
        if(isFollow){
            stringRedisTemplate.opsForSet().add(key, followUserId.toString());
        }
        return Result.ok(isFollow);
    }

    /**
     * 查询共同关注
     * @param id
     * @return
     */
    @Override
    public Result common(Long id) {
        // 1.获取当前用户id
        Long userId = UserHolder.getUser().getId();
        // 2.获取当前用户Redis中set的key
        String key1 = "follows:" + userId;
        // 3.获取目标用户Redis中set的key
        String key2 = "follows:" + id;
        // 4.查询共同集合
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key1, key2);
        if(intersect == null || intersect.isEmpty()){
            // 无交集
            return Result.ok(Collections.emptyList());
        }
        // 5.解析id集合
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        // 6.查询用户
        List<UserDTO> userDTOS = userService.listByIds(ids)
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 7.返回交集
        return Result.ok(userDTOS);
    }
}
