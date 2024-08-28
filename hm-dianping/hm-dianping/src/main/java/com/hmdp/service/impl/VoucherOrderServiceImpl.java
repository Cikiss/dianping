package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.RedisSystemException;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;
    @Resource
    private RedisIdWorker redisIdWorker;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;

    // lua脚本：判断是否具有秒杀资格
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 代理对象
    private IVoucherOrderService proxy;

    // 完成异步下单的线程
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    // 异步下单在初始化完就执行
    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        private String QUEUE_NAME = "stream.orders";
        // 完成异步下单
        @Override
        public void run() {
            // 检查并创建消费者组，避免流不存在的问题
            try {
                stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, ReadOffset.latest(), "g1");
            } catch (RedisSystemException e) {
                // 如果组已存在，忽略该错误
                if (e.getMessage().contains("BUSYGROUP")) {
                    log.info("Consumer group 'g1' already exists.");
                } else {
                    throw e;  // 其他异常继续抛出
                }
            }
            while (true){
                try {
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.lastConsumed())
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        // 2.1.获取失败，继续下一次循环
                        continue;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(QUEUE_NAME, "g1", record.getId());
                }
                catch (Exception e) {
                    log.error("处理订单异常", e);
//                    if(e instanceof RedisSystemException){
//                        stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, "g1");
//                    }else {
                        handlePendingList();
//                    }
                }
            }
        }

        // 处理PendingList中消息
        private void handlePendingList() {
            // 检查并创建消费者组，避免流不存在的问题
            try {
                stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, ReadOffset.latest(), "g1");
            } catch (RedisSystemException e) {
                // 如果组已存在，忽略该错误
                if (e.getMessage().contains("BUSYGROUP")) {
                    log.info("Consumer group 'g1' already exists.");
                } else {
                    throw e;  // 其他异常继续抛出
                }
            }
            while (true){
                try {
                    // 1.获取PendingList中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(QUEUE_NAME, ReadOffset.from("0"))
                    );
                    // 2.判断消息获取是否成功
                    if (list == null || list.isEmpty()){
                        // 2.1.获取失败，PendingList没有异常消息，结束循环
                        break;
                    }
                    // 3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 4.创建订单
                    handleVoucherOrder(voucherOrder);
                    // 5.ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge("s1", "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理PendingList订单异常", e);
//                    if(e instanceof RedisSystemException){
//                        stringRedisTemplate.opsForStream().createGroup(QUEUE_NAME, "g1");
//                    }
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    /*// 阻塞队列
    private  BlockingQueue<VoucherOrder> ORDER_TASKS = new ArrayBlockingQueue<>(1024 * 1024);
    // 下单任务内部类
    private class VoucherOrderHandler implements Runnable{
        // 完成异步下单
        @Override
        public void run() {
            while (true){
                try {
                    // 1.获取队列中的订单信息
                    VoucherOrder voucherOrder = ORDER_TASKS.take();
                    // 2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (InterruptedException e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        // 创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + voucherOrder.getUserId());
        // 获取锁
        boolean isLock = lock.tryLock();
        // 判断是否获取锁成功
        if(!isLock){
            // 获取锁失败，重试或返回错误细信息
            log.error("一人一单！");
            return;
        }
        try {
            // 保存订单
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            // 释放锁
            lock.unlock();
        }

    }

    /**
     * 秒杀
     * @param voucherId
     * @return
     */
    // Redis中的Stream实现消息队列
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 生成订单id（全局唯一id）
        long orderId = redisIdWorker.nextId("order");
        // 1.执行lua脚本
        Long longResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        // 2.判断结果是否为0
        int result = longResult.intValue();
        // 2.1.结果不为0，无购买资格，返回错误信息
        if(result != 0){
            return Result.fail(result == 1 ? "库存不足！" : "一人一单！");
        }
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
        // 4.返回订单id
        return Result.ok(orderId);
    }

    /*// BlockingQueue实现异步下单
    @Override
    public Result seckillVoucher(Long voucherId) {
        // 获取用户
        Long userId = UserHolder.getUser().getId();
        // 1.执行lua脚本
        Long longResult = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString());
        // 2.判断结果是否为0
        int result = longResult.intValue();
        // 2.1.结果不为0，无购买资格，返回错误信息
        if(result != 0){
            return Result.fail(result == 1 ? "库存不足！" : "一人一单！");
        }
        // 2.2.结果为0，有购买资格，把下单信息保存到阻塞队列
        // 2.3.生成订单
        VoucherOrder voucherOrder = new VoucherOrder();
        // 2.4.生成订单id（全局唯一id）
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        // 2.5.生成用户id
        voucherOrder.setUserId(userId);
        // 2.6.生成优惠券id
        voucherOrder.setVoucherId(voucherId);
        // 2.7.放入阻塞队列
        ORDER_TASKS.add(voucherOrder);
        // 3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        // 4.返回订单id
        return Result.ok(orderId);
    }*/

    //    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否已经开始
//        LocalDateTime now = LocalDateTime.now();
//        if(voucher.getBeginTime().isAfter(now)){
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3.判断秒杀是否已经结束
//        if(voucher.getEndTime().isBefore(now)){
//            return Result.fail("秒杀已经结束");
//        }
//        // 4.判断库存是否充足
//        if(voucher.getStock() <= 0){
//            return Result.fail("库存不足！");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//        // 创建锁对象，注意锁的粒度，每个用户一把锁，所以拼接userId
////        SimpleRedisLock lock = new SimpleRedisLock(stringRedisTemplate, "order:" + userId);
//        // Redisson
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        // 尝试获取锁
//        boolean isLock = lock.tryLock();
//        // 判断是否获取锁成功
//        if(!isLock){
//            // 获取锁失败，返回错误或重试
//            return Result.fail("一人一单！");
//        }
//        /*// 锁粒度降低，性能更好，intern()保证只锁相同的用户id值
//        synchronized (userId.toString().intern()) {*/
//        try {
//            // 获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        } finally {
//            // 释放锁
//            lock.unlock();
//        }
////        }
//    }



    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 5.一人一单
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();
        // 5.1.查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        // 5.2.判断该用户是否下过单
        if(count > 0) {
            log.error("一人一单！");
        }
        // 6.扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId).gt("stock", 0)
                .update();
        if(!success){
            log.error("库存不足！");
        }
        // 7.保存订单
        save(voucherOrder);
    }
}
