package com.hmdp.service;

import com.hmdp.dto.Result;
import com.hmdp.entity.Blog;
import com.baomidou.mybatisplus.extension.service.IService;

/**
 * <p>
 *  服务类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
public interface IBlogService extends IService<Blog> {

    /**
     * 查询最近热门博客
     * @param current
     * @return
     */
    Result queryHotBlog(Integer current);

    /**
     * 根据id查询博客
     * @param id
     * @return
     */
    Result queryBlogById(Long id);

    /**
     * 点赞
     * @param id
     * @return
     */
    Result likeBlog(Long id);

    /**
     * 查询前5名点赞者集合
     * @param id
     * @return
     */
    Result queryBlogLikes(Long id);

    /**
     * 根据用户id分页查询
     * @param id
     * @param current
     * @return
     */
    Result queryBlogByUserIdWithCurrent(Long id, Integer current);

    /**
     * 发布博客
     * @param blog
     * @return
     */
    Result saveBlog(Blog blog);

    /**
     * 查询关注用户的博客
     * @param max
     * @param offset
     * @return
     */
    Result queryBlogOfFollow(Long max, Integer offset);
}
