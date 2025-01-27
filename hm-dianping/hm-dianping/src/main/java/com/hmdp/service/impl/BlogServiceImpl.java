package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.hmdp.utils.RedisConstants.BLOG_LIKED_KEY;
import static com.hmdp.utils.RedisConstants.FEED_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {

    @Resource
    private IUserService userService;

    @Resource
    StringRedisTemplate stringRedisTemplate;

    @Resource
    private IFollowService followService;

    /**
     * 查询最近热门博客
     * @param current
     * @return
     */
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页面数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            queryBlogUser(blog);
            isBlogLiked(blog);
        });
        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    /**
     * 根据id查询博客
     * @param id
     * @return
     */
    @Override
    public Result queryBlogById(Long id) {
        // 1.查询blog
        Blog blog = getById(id);
        if(blog == null) {
            return Result.fail("博客不存在！");
        }
        // 2.查询blog有关的用户
        queryBlogUser(blog);
        // 3.查询blog是否被点赞了
        isBlogLiked(blog);
        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        String key = BLOG_LIKED_KEY + blog.getId();
        // 1.获取当前登录用户
        UserDTO user = UserHolder.getUser();
        if (user == null){
            // 用户未登录，无需查询是否点赞
            return;
        }
        Long userId = user.getId();
        // 2.判断当前登录用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(score != null);
    }

    /**
     * 一人一赞
     * @param id
     * @return
     */
    @Override
    public Result likeBlog(Long id) {
        String key = BLOG_LIKED_KEY + id;
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.判断当前登录用户是否已经点赞
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        if (score == null){
            // 3.未点赞，可以点赞
            // 3.1.数据库点赞数 + 1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            if(!isSuccess){
                return Result.fail("更新数据库失败！");
            }
            // 3.2.保存用户到Redis的zset中
            stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
        }else {
            // 4.已点赞，取消点赞
            // 4.1.数据库点赞数 - 1
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();
            if(!isSuccess){
                return Result.fail("更新数据库失败！");
            }
            // 4.2.把用户从Redis中的set集合移除
            stringRedisTemplate.opsForZSet().remove(key, userId.toString());
        }
        return Result.ok();
    }

    /**
     * 查询前5名点赞者集合
     * @param id
     * @return
     */
    @Override
    public Result queryBlogLikes(Long id) {
        // 1.查询top5的点赞用户id zrange key 0 4
        Set<String> top5Set = stringRedisTemplate.opsForZSet().range(BLOG_LIKED_KEY + id, 0, 4);
        if(top5Set == null || top5Set.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        // 2.解析用户id
        List<Long> ids = top5Set.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 3.根据用户id查询用户 WHERE id IN (5, 1) ORDER BY FIELD (id, 5, 1)
        List<UserDTO> userDTOS = userService.query()
                .in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        // 4.返回用户集合
        return Result.ok(userDTOS);
    }

    /**
     * 根据用户id分页查询
     * @param id
     * @param current
     * @return
     */
    @Override
    public Result queryBlogByUserIdWithCurrent(Long id, Integer current) {
        Page<Blog> page = query()
                .eq("user_id", id)
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        List<Blog> records = page.getRecords();
        return Result.ok(records);
    }

    /**
     * 发布博客
     * @param blog
     * @return
     */
    @Override
    public Result saveBlog(Blog blog) {
        // 1.获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2.保存探店博文
        boolean isSave = save(blog);
        if(!isSave){
            return Result.fail("新增博文失败！");
        }
        // 3.查询博文作者的所有粉丝
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        // 4.推送博文id给所有粉丝
        for (Follow follow : follows) {
            // 4.1.获取粉丝id
            Long userId = follow.getUserId();
            // 4.2.推送
            String key = FEED_KEY + userId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 5.返回博文id
        return Result.ok(blog.getId());
    }

    /**
     * 查询关注用户的博客
     * @param max
     * @param offset
     * @return
     */
    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1.获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2.查询当前用户的信箱 ZREVRANGEBYSCORE z1 Max Min LIMIT offset count
        String key = FEED_KEY + userId;
        Set<ZSetOperations.TypedTuple<String>> typedTuples =
                stringRedisTemplate.opsForZSet().reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        // 非空判断
        if(typedTuples == null || typedTuples.isEmpty()){
            return Result.ok();
        }
        // 3.解析数据：BlogId, minTime, offset
        long minTime = 0;
        int nextOffset = 1;
        List<Long> ids = new ArrayList<>(typedTuples.size());
        for (ZSetOperations.TypedTuple<String> tuple : typedTuples) {
            // 3.1.获取id，加入集合
            ids.add(Long.valueOf(tuple.getValue()));
            // 3.2.获取分数，并统计下次查询的offset
            long time = tuple.getScore().longValue();
            if(time == minTime){
                nextOffset ++;
            }else{
                minTime = time;
                nextOffset = 1;
            }
        }
        // 4.根据id查询Blog（不能使用listByIds，'in'问题）
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id, " + idStr + ")").list()
                .stream().map(blog -> {
                    queryBlogUser(blog);
                    isBlogLiked(blog);
                    return blog;
                })
                .collect(Collectors.toList());
        // 5.封装返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setOffset(nextOffset);
        r.setMinTime(minTime);
        return Result.ok(r);
    }
}
