package com.sl.ms.trade.config;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import lombok.Data;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.redisson.config.SingleServerConfig;
import org.springframework.boot.autoconfigure.data.redis.RedisProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.Resource;

@Data
@Configuration
@EnableConfigurationProperties(RedisProperties.class)
public class RedissonConfiguration {

    @Resource
    private RedisProperties redisProperties;

    @Bean
    public RedissonClient redissonSingle() {
        Config config = new Config();
        SingleServerConfig serverConfig = config.useSingleServer()
                .setAddress("redis://" + redisProperties.getHost() + ":" + redisProperties.getPort());
        if (null != (redisProperties.getTimeout())) {
            //设置持有时间
            serverConfig.setTimeout(1000 * Convert.toInt(redisProperties.getTimeout().getSeconds()));
        }
        if (StrUtil.isNotEmpty(redisProperties.getPassword())) {
            //设置密码
            serverConfig.setPassword(redisProperties.getPassword());
        }
        //创建RedissonClient
        return Redisson.create(config);
    }

}
