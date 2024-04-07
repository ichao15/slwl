package com.sl.ms.dispatch.mq;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.api.CourierFeign;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.work.api.PickupDispatchTaskFeign;
import com.sl.ms.work.domain.dto.CourierTaskCountDTO;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskType;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.vo.CourierTaskMsg;
import com.sl.transport.common.vo.OrderMsg;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.data.annotation.Id;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 订单业务消息，接收到新订单后，根据快递员的负载情况，分配快递员
 */
@Slf4j
@Component
public class OrderMQListener {

    @Resource
    private CourierFeign courierFeign;

    @Resource
    private MQFeign mqFeign;

    @Resource
    private PickupDispatchTaskFeign pickupDispatchTaskFeign;

    /**
     * 如果有多个快递员，需要查询快递员今日的取派件数，根据此数量进行计算
     * 计算的逻辑：优先分配取件任务少的，取件数相同的取第一个分配
     * <p>
     * 发送生成取件任务时需要计算时间差，如果小于2小时，实时发送；大于2小时，延时发送
     * 举例：
     * 1、现在10:30分，用户期望：11:00 ~ 12:00上门，实时发送
     * 2、现在10:30分，用户期望：13:00 ~ 14:00上门，延时发送，12点发送消息，延时1.5小时发送
     *
     * @param msg 消息内容
     */
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.DISPATCH_ORDER_TO_PICKUP_DISPATCH_TASK),
            exchange = @Exchange(name = Constants.MQ.Exchanges.ORDER_DELAYED, type = ExchangeTypes.TOPIC, delayed = Constants.MQ.DELAYED),
            key = Constants.MQ.RoutingKeys.ORDER_CREATE
    ))
    public void listenOrderMsg(String msg) {
        //{"orderId":123, "agencyId": 8001, "taskType":1, "mark":"带包装", "longitude":116.111, "latitude":39.00, "created":1654224658728, "estimatedStartTime": 1654224658728}
        log.info("接收到订单的消息 >>> msg = {}", msg);
        OrderMsg orderMsg = JSONUtil.toBean(msg, OrderMsg.class);
        //TODO day09 调度中心接收取派件任务，调度快递员

        //1. 查询消息中机构网点id
        Long agencyId = orderMsg.getAgencyId();
        //2. 获取消息中经纬度数据
        Double longitude = orderMsg.getLongitude();
        Double latitude = orderMsg.getLatitude();
        //3. 调用快递员微服务 根据条件查询作业范围内满足网点排班的快递员ids   tips: 调用courierFeign
        Long selectedCourierId = null;
        List<Long> courierIds = this.courierFeign.queryCourierIdListByCondition(agencyId, longitude, latitude, LocalDateTimeUtil.toEpochMilli(orderMsg.getEstimatedEndTime()));
        log.info("快递员微服务查出的ids：{}", courierIds);
        //4. 如果调度员列表不为空,根据规则在列表中选择任务量最少的快递员  调用selectCourier
        if (CollUtil.isNotEmpty(courierIds)) {
            //选中快递员
            selectedCourierId = this.selectCourier(courierIds, orderMsg.getTaskType());
            log.info("根据当日任务选出的快递员id：{}", selectedCourierId);
        }
        //5. 发送快递员取件消息 work服务会消费此消息 创建快递员取派件任务
        //5.1  创建任务对象 CourierTaskMsg 并补全字段
        CourierTaskMsg courierTaskMsg = CourierTaskMsg.builder()
                .courierId(selectedCourierId)
                .agencyId(agencyId)
                .taskType(orderMsg.getTaskType())
                .orderId(orderMsg.getOrderId())
                .mark(orderMsg.getMark())
                .estimatedEndTime(orderMsg.getEstimatedEndTime())
                .created(System.currentTimeMillis())
                .build();
        //5.2  计算当前时间 和 预计结束时间的时间差
        long between = LocalDateTimeUtil.between(LocalDateTime.now(), orderMsg.getEstimatedEndTime(), ChronoUnit.MINUTES);
        int delay = Constants.MQ.DEFAULT_DELAY; //默认实时发送
        //5.3  如果计算时间差大于2小时，并且是取件任务的话
        if (between > 120 && ObjectUtil.equal(orderMsg.getTaskType(), 1)) {
            //5.3.1  基于预计结束时间 -2小时得到需要通知快递的时间
            LocalDateTime sendDataTime = LocalDateTimeUtil.offset(orderMsg.getEstimatedEndTime(), -2, ChronoUnit.HOURS);
            //5.3.2   基于当前时间，和快递员应该取件的时间 时间差就是延时发送消息时间 单位：毫秒
            delay = Convert.toInt(LocalDateTimeUtil.between(LocalDateTime.now(), sendDataTime, ChronoUnit.MILLIS));
        }
        //5.4 发送消息   交换机: Constants.MQ.Exchanges.PICKUP_DISPATCH_TASK_DELAYED
        //               路由: Constants.MQ.RoutingKeys.PICKUP_DISPATCH_TASK_CREATE
        //               消息: 消息对象转jsonStr
        //              延时时间:  -1 代表不延迟
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.PICKUP_DISPATCH_TASK_DELAYED,
                Constants.MQ.RoutingKeys.PICKUP_DISPATCH_TASK_CREATE, courierTaskMsg.toJson(), delay);
    }

    /**
     * 根据当日的任务数选取快递员
     * @param courierIds 快递员列个表
     * @param taskType   任务类型
     * @return 选中的快递员id
     */
    private Long selectCourier(List<Long> courierIds, Integer taskType) {
        // 1. 如果集合长度为 1 直接返回下标0快递员id
        if (courierIds.size() == 1) {
            return courierIds.get(0);
        }
        String date = DateUtil.date().toDateStr();
        // 2. 查询当天快递员任务数量列表    tips: 基于pickupDispatchTaskFeign查询findCountByCourierIds
        List<CourierTaskCountDTO> courierTaskCountDTOS = this.pickupDispatchTaskFeign.findCountByCourierIds();
        // 3. 如果没查到任务数量  直接返回下标0快递员id
        if (CollUtil.isEmpty(courierTaskCountDTOS)) {
            return courierIds.get(0);
        }
        // 4. 查看返回任务数是否与快递员数量相同
        if (ObjectUtil.notEqual(courierIds.size(), courierTaskCountDTOS.size())) {
            // 4.1  如果不相同需要补齐，把没有数量设置任务数为0，这样就可以确保每个快递员都能分配到任务
            List<CourierTaskCountDTO> dtoList = courierIds.stream()
                    .filter(courierId -> {
                        int index = CollUtil.indexOf(courierTaskCountDTOS, dto -> ObjectUtil.equal(courierId, dto.getCourierId()));
                        return index == -1;
                    })
                    .map(courierId -> CourierTaskCountDTO.builder()
                            .courierId(courierId)
                            .count(0L).build())
                    .collect(Collectors.toList());
            // 4.2  将补0的数据加入到快递员任务数量集合中
            courierTaskCountDTOS.addAll(dtoList);
        }
        // 5. 将集合升序排序，选中任务数最小的快递员进行分配
        CollUtil.sortByProperty(courierTaskCountDTOS, "count");
        return courierTaskCountDTOS.get(0).getCourierId();
    }
}
