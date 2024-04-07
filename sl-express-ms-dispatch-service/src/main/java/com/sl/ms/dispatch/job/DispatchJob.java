package com.sl.ms.dispatch.job;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.base.api.truck.TruckPlanFeign;
import com.sl.ms.base.domain.truck.TruckDto;
import com.sl.ms.base.domain.truck.TruckPlanDto;
import com.sl.ms.dispatch.dto.DispatchMsgDTO;
import com.sl.ms.dispatch.mq.TransportOrderDispatchMQListener;
import com.sl.transport.common.constant.Constants;
import com.xxl.job.core.context.XxlJobHelper;
import com.xxl.job.core.handler.annotation.XxlJob;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 调度运输任务
 */
@Component
@Slf4j
public class DispatchJob {
    @Resource
    private TransportOrderDispatchMQListener transportOrderDispatchMQListener;
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private RedissonClient redissonClient;
    @Resource
    private TruckPlanFeign truckPlanFeign;
    @Resource
    private MQFeign mqFeign;
    @Value("${sl.volume.ratio:0.95}")
    private Double volumeRatio;
    @Value("${sl.weight.ratio:0.95}")
    private Double weightRatio;
    /**
     * 分片广播方式处理运单，生成运输任务
     */
    @XxlJob("transportTask")
    public void transportTask() {
        // 分片参数
        int shardIndex = XxlJobHelper.getShardIndex();
        int shardTotal = XxlJobHelper.getShardTotal();
        // TODO day07 定时调度任务匹配车辆、运单 生成运输任务消息

        // 1. 根据分片参数  查询2小时内并且可用状态车辆  tips: 调用truckPlanFeign远程查询
        List<TruckPlanDto> truckPlanDtoList = this.truckPlanFeign.pullUnassignedPlan(shardTotal, shardIndex);
        if (CollUtil.isEmpty(truckPlanDtoList)) {
            return;
        }
        // 2. 遍历车辆
        for (TruckPlanDto truckPlanDto : truckPlanDtoList) {
            //2.1 校验车辆计划对象 id StartOrganId EndOrganId TransportTripsId 不能为空
            if (ObjectUtil.hasEmpty(truckPlanDto.getId(), truckPlanDto.getStartOrganId(),
                    truckPlanDto.getEndOrganId(), truckPlanDto.getTransportTripsId())) {
                log.error("车辆计划对象数据不符合要求， truckPlanDto -> {}", truckPlanDto);
                continue;
            }
            //2.2 准备Redis的key
            //  根据该车辆的开始、结束机构id，来确定要处理的运单数据List集合Rediskey
            Long startOrganId = truckPlanDto.getStartOrganId();
            Long endOrganId = truckPlanDto.getEndOrganId();
            String redisKey = this.transportOrderDispatchMQListener.getListRedisKey(startOrganId, endOrganId);
            //  根据常量 + 集合RedisKey = 分布式锁RedisKey  (为了让同一用户的不同运单，尽可能的在一台车中运输 需要加分布式锁)
            String lockRedisKey = Constants.LOCKS.DISPATCH_LOCK_PREFIX + redisKey;
            RLock lock = this.redissonClient.getFairLock(lockRedisKey);
            //2.3 声明DispatchMsgDTO集合变量 (用于存储当前车辆 所要运输的所有货物基本转运信息)
            List<DispatchMsgDTO> dispatchMsgDTOList = new ArrayList<>();
            try {
                //2.4 加分布式锁  采用公平锁
                lock.lock();
                //2.5  采用递归方式   计算车辆运力 合并运单 调用: executeTransportTask
                this.executeTransportTask(redisKey, truckPlanDto.getTruckDto(), dispatchMsgDTOList);
            } finally {
                //2.6 解锁
                lock.unlock();
            }
            //2.7 基于上面的DispatchMsgDTO集合、车辆 生成运输任务 调用: createTransportTask
            this.createTransportTask(truckPlanDto, startOrganId, endOrganId, dispatchMsgDTOList);
        }
        //3. 发送消息所有查询到的车辆已经完成调度 调用: completeTruckPlan
        this.completeTruckPlan(truckPlanDtoList);
    }

    /**
     * 递归处理   判断车辆运力  匹配运单
     * @param redisKey
     * @param truckDto
     * @param dispatchMsgDTOList
     */
    private void executeTransportTask(String redisKey, TruckDto truckDto, List<DispatchMsgDTO> dispatchMsgDTOList) {
        // 1. 根据rediskey 从list队列的右侧取出数据  为空返回代表没有运单
        String redisData = stringRedisTemplate.opsForList().rightPop(redisKey);
        if (StrUtil.isEmpty(redisData)) {
            // 该车辆没有运单需要运输
            return;
        }
        // 2. 将获取到的jsonStr转为DispatchMsgDTO对象
        DispatchMsgDTO dispatchMsgDTO = JSONUtil.toBean(redisData, DispatchMsgDTO.class);
        // 3. 计算该车辆已经分配的运单，是否超出其运力，载重 或 体积超出，需要将新拿到的运单加进去后进行比较
        //    计算总重量: dispatchMsgDTOList已经装车的 + 新拿到的运单
        BigDecimal totalWeight = NumberUtil.add(NumberUtil.toBigDecimal(dispatchMsgDTOList.stream()
                .mapToDouble(DispatchMsgDTO::getTotalWeight)
                .sum()), dispatchMsgDTO.getTotalWeight());
        //    计算总体积: dispatchMsgDTOList已经装车的 + 新拿到的运单
        BigDecimal totalVolume = NumberUtil.add(NumberUtil.toBigDecimal(dispatchMsgDTOList.stream().
                mapToDouble(DispatchMsgDTO::getTotalVolume)
                .sum()), dispatchMsgDTO.getTotalVolume());
        //4. 车辆最大的容积和载重要留有余量，否则可能会超重 或 装不下
        //    实际可容纳最大重量 = AllowableLoad  *  weightRatio
        BigDecimal maxAllowableLoad = NumberUtil.mul(truckDto.getAllowableLoad(), weightRatio);
        //    实际可容纳最大体积 = AllowableVolume * volumeRatio
        BigDecimal maxAllowableVolume = NumberUtil.mul(truckDto.getAllowableVolume(), volumeRatio);
        //    如果当前 计算总重量 >= 实际可容纳最大重量  或者 计算总体积 >=实际可容纳最大体积
        if (NumberUtil.isGreaterOrEqual(totalWeight, maxAllowableLoad)
                || NumberUtil.isGreaterOrEqual(totalVolume, maxAllowableVolume)) {
            // 那么超出车辆运力，需要取货的运单再放回Redis去，放到最右边，以便保证运单处理的顺序  并Return结束方法
            stringRedisTemplate.opsForList().rightPush(redisKey, redisData);
            return;
        }
        //5. 没有超出运力，将该运单加入到dispatchMsgDTOList集合中,代表已装运单
        dispatchMsgDTOList.add(dispatchMsgDTO);
        //6. 递归处理运单
        this.executeTransportTask(redisKey, truckDto, dispatchMsgDTOList);
    }

    /**
     * 发送生成运输任务消息
     * @param truckPlanDto
     * @param startOrganId
     * @param endOrganId
     * @param dispatchMsgDTOList
     */
    private void createTransportTask(TruckPlanDto truckPlanDto, Long startOrganId, Long endOrganId, List<DispatchMsgDTO> dispatchMsgDTOList) {
        //将运单车辆的结果以消息的方式发送出去
        //消息格式:
        // {"driverId":[], "truckPlanId":456, "truckId":1210114964812075008,
        // "totalVolume":4.2,"endOrganId":90001,"totalWeight":7,
        // "transportOrderIdList":[320733749248,420733749248],"startOrganId":100280}

        // 1. 在运单货物列表中提取出运单ID集合
        List<String> transportOrderIdList = CollUtil.getFieldValues(dispatchMsgDTOList, "transportOrderId", String.class);
        // 2. 获取司机id列表  tips: 确保不为null
        List<Long> driverIds = CollUtil.isNotEmpty(truckPlanDto.getDriverIds()) ? truckPlanDto.getDriverIds() : ListUtil.empty();
        // 3. 构建消息map 需要的key参照上面消息格式
        Map<String, Object> msgResult = MapUtil.<String, Object>builder()
                .put("truckId", truckPlanDto.getTruckId()) //车辆id
                .put("driverIds", driverIds) //司机id
                .put("truckPlanId", truckPlanDto.getId()) //车辆计划id
                .put("transportTripsId", truckPlanDto.getTransportTripsId()) //车次id
                .put("startOrganId", startOrganId) //开始机构id
                .put("endOrganId", endOrganId) //结束机构id
                //运单id列表
                .put("transportOrderIdList", transportOrderIdList)
                //总重量
                .put("totalWeight", dispatchMsgDTOList.stream()
                        .mapToDouble(DispatchMsgDTO::getTotalWeight)
                        .sum())
                //总体积
                .put("totalVolume", dispatchMsgDTOList.stream()
                        .mapToDouble(DispatchMsgDTO::getTotalVolume)
                        .sum()).build();
        // 4. 将消息map转为jsonStr 并发送消息  交换机: Constants.MQ.Exchanges.TRANSPORT_TASK  路由: Constants.MQ.RoutingKeys.TRANSPORT_TASK_CREATE
        String jsonMsg = JSONUtil.toJsonStr(msgResult);
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_TASK,
                Constants.MQ.RoutingKeys.TRANSPORT_TASK_CREATE, jsonMsg);
        // 5. 如果运单id列表不为空，需要删除redis中set集合用来判断重复的对应数据
        if (CollUtil.isNotEmpty(transportOrderIdList)) {
            String setRedisKey = this.transportOrderDispatchMQListener.getSetRedisKey(startOrganId, endOrganId);
            this.stringRedisTemplate.opsForSet().remove(setRedisKey, transportOrderIdList.toArray());
        }
    }

    /**
     * 发送车辆调度完成消息，用于通知base服务对车辆状态 做后续变更
     * @param truckDtoList
     */
    private void completeTruckPlan(List<TruckPlanDto> truckDtoList) {
        //{"ids":[1,2,3], "created":123456}
        Map<String, Object> msg = MapUtil.<String, Object>builder()
                .put("ids", CollUtil.getFieldValues(truckDtoList, "id", Long.class))
                .put("created", System.currentTimeMillis()).build();
        String jsonMsg = JSONUtil.toJsonStr(msg);
        //发送消息
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRUCK_PLAN,
                Constants.MQ.RoutingKeys.TRUCK_PLAN_COMPLETE, jsonMsg);
    }
}
