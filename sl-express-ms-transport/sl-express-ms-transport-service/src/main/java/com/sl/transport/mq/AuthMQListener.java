package com.sl.transport.mq;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.gson.JsonObject;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.entity.node.AgencyEntity;
import com.sl.transport.entity.node.BaseEntity;
import com.sl.transport.entity.node.OLTEntity;
import com.sl.transport.entity.node.TLTEntity;
import com.sl.transport.enums.OrganTypeEnum;
import com.sl.transport.service.IService;
import com.sl.transport.utils.OrganServiceFactory;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.core.TopicExchange;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

/**
 * 对于权限管家系统消息的处理
 */
@Slf4j
@Component
public class AuthMQListener {
    /**
     * 监听权限管家中机构的变更消息
     * @param msg
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.AUTH_TRANSPORT),
            exchange = @Exchange(name = "${rabbitmq.exchange}", type = ExchangeTypes.TOPIC),
            key = "#"
    ))
    public void listenAgencyMsg(String msg) {
        //{"type":"ORG","operation":"ADD",
        // "content":[{"id":"977263044792942657","name":"55",
        // "parentId":"0","managerId":null,"status":true}]}
        log.info("接收到消息 -> {}", msg);

        //TODO day05 机构同步功能

        // 获取type的值，如果不为ORG 直接return  (ORG代表机构变更消息)
        JSONObject jsonObject = JSONUtil.parseObj(msg);
        String type = jsonObject.getStr("type");
        if (!StrUtil.equalsIgnoreCase(type, "ORG")) {
            return;
        }
        // 获取json中其它值 operation操作类型 content是数组取0号下标
        String operation = jsonObject.getStr("operation");
        JSONObject content = (JSONObject) jsonObject.getJSONArray("content").get(0);
        // 取到content中的元素后 获取 name机构名称  parentId父机构ID
        String name = content.getStr("name");
        Long parentId = content.getLong("parentId");
        // 定义 IService、BaseEntity变量 (因为还不确定具体使用什么实现，所以先定义好父接口和父实体类)
        IService service;
        BaseEntity entity;
        // 如果name以转运中心结尾，
        //          基于OrganServiceFactory工厂类获取转运中心操作service对象
        //          new 一级转运中心实体类对象 并设置parentId为0
        if (StrUtil.endWith(name, "转运中心")) {
            service = OrganServiceFactory.getBean(OrganTypeEnum.OLT.getCode());
            entity = new OLTEntity();
            entity.setParentId(0L);
        } else if (name.endsWith("分拣中心")) {
            // 如果name以分拣中心结尾,
            //          基于OrganServiceFactory工厂类获取分拣中心操作service对象
            //          new 二级分拣中心实体类对象 并设置parentId
            service = OrganServiceFactory.getBean(OrganTypeEnum.TLT.getCode());
            entity = new TLTEntity();
            entity.setParentId(parentId);
        } else if (name.endsWith("营业部")) {
            // 如果name以营业部结尾,
            //          基于OrganServiceFactory工厂类获取营业部操作service对象
            //          new 三级网点实体类对象 并设置parentId
            service = OrganServiceFactory.getBean(OrganTypeEnum.AGENCY.getCode());
            entity = new AgencyEntity();
            entity.setParentId(parentId);
        } else {
            // 都不是 直接return  不处理此消息
            return;
        }

        // 补全实体类其它参数 bid取机构id  name  status
        entity.setBid(content.getLong("id"));
        entity.setName(name);
        entity.setStatus(content.getBool("status"));
        // 根据不同的操作类型，调用IService的不同方法实现
        switch (operation) {
            // 如果operation是ADD，则调用添加机构方法
            case "ADD":
                service.create(entity);
                break;
            // 如果operation是UPDATE，则调用修改机构方法
            case "UPDATE":
                service.update(entity);
                break;
            // 如果operation是DEL，则调用删除机构方法
            case "DEL":
                service.deleteByBid(entity.getBid());
                break;
        }
    }
}