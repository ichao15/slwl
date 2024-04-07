package com.sl.gateway.auth;

import cn.hutool.json.JSONUtil;
import com.itheima.auth.boot.autoconfigure.AuthorityProperties;
import com.itheima.auth.factory.AuthTemplateFactory;
import com.itheima.auth.sdk.AuthTemplate;
import com.itheima.auth.sdk.common.Result;
import com.itheima.auth.sdk.dto.AuthUserInfoDTO;
import com.itheima.auth.sdk.dto.LoginDTO;
import com.itheima.auth.sdk.dto.UserDTO;
import com.itheima.auth.sdk.service.TokenCheckService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;

/**
 * @author zzj
 * @version 1.0
 */
@SpringBootTest(properties = "spring.main.web-application-type = reactive")
public class AuthTemplateTest {

    @Resource
    private AuthTemplate authTemplate;
    @Resource
    private TokenCheckService tokenCheckService;
    @Autowired
    private AuthorityProperties authorityProperties;

    @Test
    public void testLogin() {
        //登录
        Result<LoginDTO> result = this.authTemplate.opsForLogin()
                .token("shenlingadmin", "123456");

        String token = result.getData().getToken().getToken();
        System.out.println("token为：" + token);

        UserDTO user = result.getData().getUser();
        System.out.println("user信息：" + user);

        //查询角色
        Result<List<Long>> resultRole = AuthTemplateFactory.get(token).opsForRole()
                .findRoleByUserId(user.getId());
        System.out.println(resultRole);
    }

    @Test
    public void checkToken() {
        //上面方法中生成的token
        String token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiIxMDI0NzA1NzA5MjU1NzczMzQ1IiwiYWNjb3VudCI6InNoZW5saW5nYWRtaW4iLCJuYW1lIjoi56We6aKG566h55CG5ZGYIiwib3JnaWQiOjEwMjQ3MDQ4NDQ0ODY3NTY2NDEsInN0YXRpb25pZCI6MTAyNDcwNTQ4OTQzNjQ5NDcyMSwiYWRtaW5pc3RyYXRvciI6ZmFsc2UsImV4cCI6MTcwNzg0Nzc0MX0.LIy2fSDEAjXU9uifEk-VJtyMu64c4Zzzs7JWDo_6XZMguZwJ9cEU1CopGIzcU1uYlbRo-prdmiqdn-2SJ-v_pg";
        AuthUserInfoDTO authUserInfo = this.tokenCheckService.parserToken(token);
        System.out.println(authUserInfo);

        System.out.println(JSONUtil.toJsonStr(authUserInfo));
    }
}
