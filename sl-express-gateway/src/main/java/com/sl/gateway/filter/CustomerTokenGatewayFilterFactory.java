package com.sl.gateway.filter;

import cn.hutool.core.convert.Convert;
import com.itheima.auth.sdk.dto.AuthUserInfoDTO;
import com.sl.gateway.config.MyConfig;
import com.sl.gateway.constant.JwtClaimsConstant;
import com.sl.gateway.properties.JwtProperties;
import com.sl.gateway.util.JwtUtil;
import io.jsonwebtoken.Claims;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * 用户端token拦截处理
 */
@Slf4j
@Component
public class CustomerTokenGatewayFilterFactory extends AbstractGatewayFilterFactory<Object> implements AuthFilter {

    @Resource
    private MyConfig myConfig;

    @Resource
    private JwtProperties jwtProperties;

    @Value("sl.jwt.user-secret-key")
    private String secretKey;

    @Override
    public GatewayFilter apply(Object config) {
        return new TokenGatewayFilter(this.myConfig, this);
    }

    @Override
    public AuthUserInfoDTO check(String token) {

        //TODO day01 权限认证 作业03

        try {
            // 基于JwtUtil解析token获取Claims内容
            Claims claims = JwtUtil.parseJWT(secretKey, token);
            // 在解析的内容中获取用户ID
            Long userId = claims.get("userId", Long.class);
            // 封装AuthUserInfoDTO对象返回(只设置id即可)
            AuthUserInfoDTO authUserInfoDTO = new AuthUserInfoDTO();
            authUserInfoDTO.setUserId(userId);
            // 注意捕获异常，如果出现异常返回null即可
            return authUserInfoDTO;
        } catch (Exception e) {
            log.error(">>>>>>>>>>>>>解析用户登录token失败<<<<<<<<<<<<<<<");
            return null;
        }
    }

    @Override
    public Boolean auth(String token, AuthUserInfoDTO authUserInfoDTO, String path) {
        //普通用户不需要校验角色
        return true;

    }
}
