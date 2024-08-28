package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;
import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送手机验证码（基于session）
     * @param phone
     * @param session
     * @return
     */
//    @Override
//    public Result sendCode(String phone, HttpSession session) {
//        // 1.校验手机号
//        if (RegexUtils.isPhoneInvalid(phone)) {
//            // 2.不合法返回错误
//            return Result.fail("手机号格式错误");
//        }
//        // 3.合法，生成验证码
//        String code = RandomUtil.randomNumbers(6);
//        // 4.保存到session
//        session.setAttribute(phone, code);
//        // 5.发送验证码
//        log.info("手机验证码：{}", code);
//        return Result.ok(code);
//    }

    /**
     * 基于redis
     * @param phone
     * @param session
     * @return
     */
    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.不合法返回错误
            return Result.fail("手机号格式错误");
        }
        // 3.合法，生成验证码
        String code = RandomUtil.randomNumbers(6);
        // 4.保存到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone, code, LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 5.发送验证码
        log.info("手机验证码：{}", code);
        return Result.ok(code);
    }

    /**
     * 用户登录
     * @param loginForm
     * @param session
     * @return
     */
//    @Override
//    public Result login(LoginFormDTO loginForm, HttpSession session) {
//
//        String code = loginForm.getCode();
//        String phone = loginForm.getPhone();
//        // 1.校验手机号
//        if(RegexUtils.isPhoneInvalid(phone)){
//            return Result.fail("手机号格式错误");
//        }
//        // 2.校验验证码
//        Object cacheCode = session.getAttribute(phone);
//        if(cacheCode == null || !cacheCode.toString().equals(code)){
//            // 3.不合法，返回错误信息
//            return Result.fail("验证码错误");
//        }
//        // 4.合法，根据手机号查询用户
//        User user = query().eq("phone", phone).one();
//        // 5.用户存在，跳过
//        // 6.用户不存在
//        if(user == null){
//            // 6.1 注册
//            user = createUserWithPhone(phone);
//        }
//        // 7 保存到session中
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
//        return Result.ok();
//    }

    /**
     * 基于redis
     * @param loginForm
     * @param session
     * @return
     */
    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {

        String code = loginForm.getCode();
        String phone = loginForm.getPhone();
        // 1.校验手机号
        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }
        // 2.校验验证码
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if(cacheCode == null || !cacheCode.equals(code)){
            // 3.不合法，返回错误信息
            return Result.fail("验证码错误");
        }
        // 4.合法，根据手机号查询用户
        User user = query().eq("phone", phone).one();
        // 5.用户存在，跳过
        // 6.用户不存在
        if(user == null){
            // 6.1 注册
            user = createUserWithPhone(phone);
        }
        // 7.保存到Redis中
        // 7.1.随机生成token，作为登录令牌
        String token = UUID.randomUUID().toString(true);
        // 7.2.将User对象转为HashMap存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName, fieldValue) -> fieldValue.toString()));
        // 7.3.存储
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
        // 7.4.设置token有效期
        stringRedisTemplate.expire(tokenKey, LOGIN_USER_TTL, TimeUnit.MINUTES);
        // 8.返回token
        return Result.ok(token);
    }

    /**
     * 根据手机号创建用户
     * @param phone
     * @return
     */
    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));
        save(user);
        return user;
    }

    /**
     * 根据id查用户
     * @param userId
     * @return
     */
    @Override
    public Result queryUserById(Long userId) {
        User user = getById(userId);
        if(user == null) {
            return Result.ok();
        }
        UserDTO userDTO =  BeanUtil.copyProperties(user, UserDTO.class);
        return Result.ok(userDTO);
    }

    /**
     * 签到
     * @return
     */
    @Override
    public Result sign() {
        // 1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            return Result.fail("未登录！");
        }
        Long userId = user.getId();
        // 2.获取当前时间
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.写入Redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    /**
     * 统计本月连续签到天数
     * @return
     */
    @Override
    public Result singCount() {
        // 1.获取当前用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            return Result.fail("未登录！");
        }
        Long userId = user.getId();
        // 2.获取当前日期
        LocalDateTime now = LocalDateTime.now();
        // 3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        // 4.获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        // 5.获取截止今天为止的所有签到记录，返回十进制数
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );
        if(result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if(num == null || num == 0){
            return Result.ok(0);
        }
        // 6.得到连续签到天数
        int count = 0;
        while(num > 0){
            if ((num & 1) == 0){
                break;
            }else{
                count ++;
            }
            num >>>= 1;
        }
        // 7.返回
        return Result.ok(count);
    }
}
