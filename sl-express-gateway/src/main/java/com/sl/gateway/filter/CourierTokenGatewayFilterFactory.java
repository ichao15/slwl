package com.sl.gateway.filter;

import cn.hutool.core.collection.CollUtil;
import com.itheima.auth.factory.AuthTemplateFactory;
import com.itheima.auth.sdk.AuthTemplate;
import com.itheima.auth.sdk.common.AuthSdkException;
import com.itheima.auth.sdk.common.Result;
import com.itheima.auth.sdk.dto.AuthUserInfoDTO;
import com.itheima.auth.sdk.service.TokenCheckService;
import com.sl.gateway.config.MyConfig;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilter;
import org.springframework.cloud.gateway.filter.factory.AbstractGatewayFilterFactory;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Collection;
import java.util.List;

/**
 * 快递员token拦截处理
 */
@Component
@Slf4j
public class CourierTokenGatewayFilterFactory extends AbstractGatewayFilterFactory<Object> implements AuthFilter {

    @Resource
    private MyConfig myConfig;
    @Resource
    private TokenCheckService tokenCheckService;
    @Value("${role.courier}")
    private List<Long> courierRoleIds;

    @Override
    public GatewayFilter apply(Object config) {
        return new TokenGatewayFilter(this.myConfig, this);
    }

    @Override
    public AuthUserInfoDTO check(String token) {
        try {
            //校验token
            return tokenCheckService.parserToken(token);
        } catch (AuthSdkException e) {
            // 校验失败
        }
        return null;
    }

    @Override
    public Boolean auth(String token, AuthUserInfoDTO authUserInfoDTO, String path) {

        //TODO day01 权限认证 作业01

        // 获取AuthTemplate对象   tips: AuthTemplateFactory可获取
        AuthTemplate authTemplate = AuthTemplateFactory.get(token);
        // 查询登陆用户对应角色ids
        List<Long> roleIds = authTemplate.opsForRole().findRoleByUserId(authUserInfoDTO.getUserId()).getData();
        // 和配置的访问角色 取交集
        // 判断是否有交集即可判断出是否有权限
        Collection<Long> intersection = CollUtil.intersection(roleIds, this.courierRoleIds);
        return CollUtil.isNotEmpty(intersection);
    }
}
