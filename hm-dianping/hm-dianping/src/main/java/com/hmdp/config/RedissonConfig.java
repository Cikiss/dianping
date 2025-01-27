package com.hmdp.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RedissonConfig {
    @Bean
    public RedissonClient redissonClient(){
        // 配置类
        Config config = new Config();
        // 添加Redis地址，单点，集群：config.useClusterServers()
        config.useSingleServer().setAddress("redis://192.168.174.129:6379").setPassword("123456");
        return Redisson.create(config);
    }
}
