package com.sl.ms.work.mq;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.sl.ms.base.api.truck.TruckPlanFeign;
import com.sl.ms.base.domain.truck.TruckPlanDto;
import com.sl.ms.driver.api.DriverJobFeign;
import com.sl.ms.transport.api.TransportLineFeign;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderSchedulingStatus;
import com.sl.ms.work.domain.enums.transporttask.TransportTaskAssignedStatus;
import com.sl.ms.work.domain.enums.transporttask.TransportTaskLoadingStatus;
import com.sl.ms.work.domain.enums.transporttask.TransportTaskStatus;
import com.sl.ms.work.entity.TransportOrderEntity;
import com.sl.ms.work.entity.TransportOrderTaskEntity;
import com.sl.ms.work.entity.TransportTaskEntity;
import com.sl.ms.work.service.TransportOrderService;
import com.sl.ms.work.service.TransportOrderTaskService;
import com.sl.ms.work.service.TransportTaskService;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.domain.TransportLineDTO;
import com.sl.transport.domain.TransportLineSearchDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName 类名
 * @Description 类说明
 */
@Slf4j
@Component
public class TransportTaskMQListener {
    @Resource
    private DriverJobFeign driverJobFeign;
    @Resource
    private TruckPlanFeign truckPlanFeign;
    @Resource
    private TransportLineFeign transportLineFeign;
    @Resource
    private TransportTaskService transportTaskService;
    @Resource
    private TransportOrderTaskService transportOrderTaskService;
    @Resource
    private TransportOrderService transportOrderService;
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.WORK_TRANSPORT_TASK_CREATE),
            exchange = @Exchange(name = Constants.MQ.Exchanges.TRANSPORT_TASK, type = ExchangeTypes.TOPIC),
            key = Constants.MQ.RoutingKeys.TRANSPORT_TASK_CREATE
    ))
    public void listenTransportTaskMsg(String msg) {
        //解析消息 {"driverIds":[123,345], "truckPlanId":456, "truckId":1210114964812075008,"totalVolume":4.2,"endOrganId":90001,"totalWeight":7,"transportOrderIdList":[320733749248,420733749248],"startOrganId":100280}
        JSONObject jsonObject = JSONUtil.parseObj(msg);
        //获取到司机id列表
        JSONArray driverIds = jsonObject.getJSONArray("driverIds");
        // 分配状态
        TransportTaskAssignedStatus assignedStatus = CollUtil.isEmpty(driverIds) ? TransportTaskAssignedStatus.MANUAL_DISTRIBUTED : TransportTaskAssignedStatus.DISTRIBUTED;

        //创建运输任务
        // TODO day07 接收运输调度消息，生成运输任务并返回运输任务ID   调用createTransportTask方法
        Long transportTaskId = this.createTransportTask(jsonObject, assignedStatus) ;

        if (CollUtil.isEmpty(driverIds)) {
            log.info("生成司机作业单，司机列表为空，需要手动设置司机作业单 -> msg = {}", msg);
            return;
        }
        for (Object driverId : driverIds) {
            //调用司机服务  ， 根据运输任务ID 生成司机作业单
            this.driverJobFeign.createDriverJob(transportTaskId, Convert.toLong(driverId));
        }
    }

    /**
     *  运输状态枚举: {@link TransportTaskStatus }
     *  载重状态枚举: {@link TransportTaskLoadingStatus}
     * @param jsonObject 消息json数据
     * @param assignedStatus 任务分配状态
     * @return
     */
    @Transactional
    protected Long createTransportTask(JSONObject jsonObject, TransportTaskAssignedStatus assignedStatus) {
        // TODO day07 创建运输任务

        // 1 根据车辆计划id truckPlanId查询车辆计划实体数据  tips: 调用truckPlanFeign
        Long truckPlanId = jsonObject.getLong("truckPlanId");
        TruckPlanDto truckPlanDto = truckPlanFeign.findById(truckPlanId);
        // 2 创建运输任务实体对象
        //      补全属性: TruckPlanId TruckId StartAgencyId EndAgencyId TransportTripsId
        TransportTaskEntity transportTaskEntity = new TransportTaskEntity();
        transportTaskEntity.setTruckPlanId(jsonObject.getLong("truckPlanId"));
        transportTaskEntity.setTruckId(jsonObject.getLong("truckId"));
        transportTaskEntity.setStartAgencyId(jsonObject.getLong("startOrganId"));
        transportTaskEntity.setEndAgencyId(jsonObject.getLong("endOrganId"));
        transportTaskEntity.setTransportTripsId(jsonObject.getLong("transportTripsId"));
        transportTaskEntity.setAssignedStatus(assignedStatus); //任务分配状态
        transportTaskEntity.setPlanDepartureTime(truckPlanDto.getPlanDepartureTime()); //计划发车时间
        transportTaskEntity.setPlanArrivalTime(truckPlanDto.getPlanArrivalTime()); //计划到达时间
        //   AssignedStatus PlanDepartureTime PlanArrivalTime Status运输状态: 待执行
        transportTaskEntity.setStatus(TransportTaskStatus.PENDING); //设置运输任务状态
        //  根据数据中是否有运单  transportOrderIdList，设置运输任务负载状态  简化处理: 有订单满载 没有订单空载
        if (CollUtil.isEmpty(jsonObject.getJSONArray("transportOrderIdList"))) {
            transportTaskEntity.setLoadingStatus(TransportTaskLoadingStatus.EMPTY);
        } else {
            transportTaskEntity.setLoadingStatus(TransportTaskLoadingStatus.FULL);
        }
        //   查询路线距离   说明: 没有专门对应的方法，需要使用分页查询transportLineFeign.queryPageList一条数据，获取里面的距离属性
        TransportLineSearchDTO transportLineSearchDTO = new TransportLineSearchDTO();
        transportLineSearchDTO.setPage(1);
        transportLineSearchDTO.setPageSize(1);
        transportLineSearchDTO.setStartOrganId(transportTaskEntity.getStartAgencyId());
        transportLineSearchDTO.setEndOrganId(transportTaskEntity.getEndAgencyId());
        PageResponse<TransportLineDTO> transportLineResponse = this.transportLineFeign.queryPageList(transportLineSearchDTO);
        TransportLineDTO transportLineDTO = CollUtil.getFirst(transportLineResponse.getItems());
        if (ObjectUtil.isNotEmpty(transportLineDTO)) {
            //设置距离
            transportTaskEntity.setDistance(transportLineDTO.getDistance());
        }
        //3  保存运输任务数据
        this.transportTaskService.save(transportTaskEntity);
        //4  创建运输任务与运单之间的关系  调用createTransportOrderTask
        this.createTransportOrderTask(transportTaskEntity.getId(), jsonObject);
        //5. 返回运输任务的id,供其他业务使用
        return transportTaskEntity.getId();
    }
    private void createTransportOrderTask(final Long transportTaskId, final JSONObject jsonObject) {
        // TODO day07 创建运输任务 和 运单的关联关系，并修改运单调度状态
        //1. 获取此次任务中涉及运单的ID列表  transportOrderIdList
        JSONArray transportOrderIdList = jsonObject.getJSONArray("transportOrderIdList");
        if (CollUtil.isEmpty(transportOrderIdList)) {
            return;
        }
        //2. 将运单id列表转成运单实体列表 transportOrderIdList ==> List<TransportOrderTaskEntity>
        List<TransportOrderTaskEntity> resultList = transportOrderIdList.stream()
                .map(o -> {
                    TransportOrderTaskEntity transportOrderTaskEntity = new TransportOrderTaskEntity();
                    transportOrderTaskEntity.setTransportTaskId(transportTaskId);
                    transportOrderTaskEntity.setTransportOrderId(Convert.toStr(o));
                    return transportOrderTaskEntity;
                }).collect(Collectors.toList());

        //3. 批量保存运输任务与运单的关联表
        this.transportOrderTaskService.batchSaveTransportOrder(resultList);
        //4. 批量标记运单为已调度状态 tips: 遍历运单id列表， 封装TransportOrderEntity实体 只设置id和调度状态属性
        List<TransportOrderEntity> list = transportOrderIdList.stream()
                .map(o -> {
                    TransportOrderEntity transportOrderEntity = new TransportOrderEntity();
                    transportOrderEntity.setId(Convert.toStr(o));
                    //状态设置为已调度
                    transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.SCHEDULED);
                    return transportOrderEntity;
                }).collect(Collectors.toList());
        //5. 调用运单service批量修改方法
        this.transportOrderService.updateBatchById(list);
    }
}
