package com.sl.ms.work.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.date.LocalDateTimeUtil;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.oms.api.OrderFeign;
import com.sl.ms.oms.enums.OrderStatus;
import com.sl.ms.work.domain.dto.CourierTaskCountDTO;
import com.sl.ms.work.domain.dto.PickupDispatchTaskDTO;
import com.sl.ms.work.domain.dto.request.PickupDispatchTaskPageQueryDTO;
import com.sl.ms.work.domain.enums.WorkExceptionEnum;
import com.sl.ms.work.domain.enums.pickupDispatchtask.*;
import com.sl.ms.work.entity.PickupDispatchTaskEntity;
import com.sl.ms.work.mapper.TaskPickupDispatchMapper;
import com.sl.ms.work.service.PickupDispatchTaskService;
import com.sl.ms.work.service.TransportOrderService;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.common.vo.OrderMsg;
import javassist.expr.NewArray;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName 类名
 * @Description 类说明
 */
@Slf4j
@Service
public class PickupDispatchTaskServiceImpl extends ServiceImpl<TaskPickupDispatchMapper, PickupDispatchTaskEntity> implements PickupDispatchTaskService {

    @Resource
    private TransportOrderService transportOrderService;

    @Resource
    private TaskPickupDispatchMapper taskPickupDispatchMapper;

    @Resource
    private OrderFeign orderFeign;

    /**
     * 任务状态: {@link PickupDispatchTaskStatus}
     *
     * 取消原因: {@link PickupDispatchTaskCancelReason}
     *
     * 运单状态: {@link OrderStatus}
     * @param pickupDispatchTaskDTO 修改的数据
     * @return
     */
    @Override
    @Transactional
    public Boolean updateStatus(PickupDispatchTaskDTO pickupDispatchTaskDTO) {
        //TODO day09 修改取派件任务状态

        //1. 校验: id 和 状态不能为空  否则抛异常
        WorkExceptionEnum paramError = WorkExceptionEnum.PICKUP_DISPATCH_TASK_PARAM_ERROR;
        if (ObjectUtil.hasEmpty(pickupDispatchTaskDTO.getId(), pickupDispatchTaskDTO.getStatus())) {
            throw new SLException("更新取派件任务状态，id或status不能为空", paramError.getCode());
        }
        //2. 根据id查询取派件任务
        PickupDispatchTaskEntity pickupDispatchTask = super.getById(pickupDispatchTaskDTO.getId());
        switch (pickupDispatchTaskDTO.getStatus()) {
            //3. 如果任务状态为NEW  抛出异常 (修改状态方法不允许)
            case NEW: {
                throw new SLException(WorkExceptionEnum.PICKUP_DISPATCH_TASK_STATUS_NOT_NEW);
            }
            //4. 如果任务状态为COMPLETED
            case COMPLETED: {
                //4.1 设置任务状态为已完成
                pickupDispatchTask.setStatus(PickupDispatchTaskStatus.COMPLETED);
                //4.2 设置实际结束时间为当前时间
                pickupDispatchTask.setActualEndTime(LocalDateTime.now());
                //4.3 如果任务状态为派件任务
                if (PickupDispatchTaskType.DISPATCH == pickupDispatchTask.getTaskType()) {
                    //4.3.1 需要设置签收状态 和 签收人
                    //如果是派件任务的完成，已签收需要设置签收状态和签收人，拒收只需要设置签收状态
                    if (ObjectUtil.isEmpty(pickupDispatchTaskDTO.getSignStatus())) {
                        throw new SLException("完成派件任务，签收状态不能为空", paramError.getCode());
                    }
                    pickupDispatchTask.setSignStatus(pickupDispatchTaskDTO.getSignStatus());
                    if (PickupDispatchTaskSignStatus.RECEIVED == pickupDispatchTaskDTO.getSignStatus()) {
                        if (ObjectUtil.isEmpty(pickupDispatchTaskDTO.getSignRecipient())) {
                            throw new SLException("完成派件任务，签收人不能为空", paramError.getCode());
                        }
                        pickupDispatchTask.setSignRecipient(pickupDispatchTaskDTO.getSignRecipient());
                    }
                }
                break;
            }
            //5. 如果任务状态取消CANCELLED
            case CANCELLED: {
                if (ObjectUtil.isEmpty(pickupDispatchTaskDTO.getCancelReason())) {
                    throw new SLException("取消任务，原因不能为空", paramError.getCode());
                }
                //5.1 设置状态为取消状态
                pickupDispatchTask.setStatus(PickupDispatchTaskStatus.CANCELLED);
                //5.2 设置取消原因 取消原因描述  取消时间
                pickupDispatchTask.setCancelReason(pickupDispatchTaskDTO.getCancelReason());
                pickupDispatchTask.setCancelReasonDescription(pickupDispatchTaskDTO.getCancelReasonDescription());
                pickupDispatchTask.setCancelTime(LocalDateTime.now());
                //5.3 如果取消原因 为: 因快递员原因无法取件
                if (pickupDispatchTaskDTO.getCancelReason() == PickupDispatchTaskCancelReason.RETURN_TO_AGENCY) {
                    //5.3.1  重新发送待取件消息到调度中心 (后续触发重新派件操作)   sendPickupDispatchTaskMsgAgain
                    OrderMsg orderMsg = OrderMsg.builder()
                            .agencyId(pickupDispatchTask.getAgencyId())
                            .orderId(pickupDispatchTask.getOrderId())
                            .created(DateUtil.current())
                            .taskType(PickupDispatchTaskType.PICKUP.getCode()) //取件任务
                            .mark(pickupDispatchTask.getMark())
                            .estimatedEndTime(pickupDispatchTask.getEstimatedEndTime()).build();
                    this.transportOrderService.sendPickupDispatchTaskMsgToDispatch(null, orderMsg);
                } else if (pickupDispatchTaskDTO.getCancelReason() == PickupDispatchTaskCancelReason.CANCEL_BY_USER){
                    //5.4 如果取消原因 为: 用户主动取消  远程调用订单服务修改订单状态为取消状态
                    orderFeign.updateStatus(ListUtil.of(pickupDispatchTask.getOrderId()), OrderStatus.CANCELLED.getCode());
                } else {
                    //5.5 如果是其它取消原因  远程调用订单服务修改订单状态为关闭运单状态
                    orderFeign.updateStatus(ListUtil.of(pickupDispatchTask.getOrderId()), OrderStatus.CLOSE.getCode());
                }
                break;
            }
            default: {
                throw new SLException("其他未知状态，不能完成更新操作", paramError.getCode());
            }
        }
        //6 根据ID修改取快件任务
        //TODO 发送消息，同步更新快递员任务
        return super.updateById(pickupDispatchTask);
    }

    @Override
    public Boolean updateCourierId(Long id, Long originalCourierId, Long targetCourierId) {
        //TODO day09 改派快递员
        if (ObjectUtil.hasEmpty(id, targetCourierId, originalCourierId)) {
            throw new SLException(WorkExceptionEnum.UPDATE_COURIER_PARAM_ERROR);
        }
        if (ObjectUtil.equal(originalCourierId, targetCourierId)) {
            throw new SLException(WorkExceptionEnum.UPDATE_COURIER_EQUAL_PARAM_ERROR);
        }
        PickupDispatchTaskEntity pickupDispatchTask = super.getById(id);
        if (ObjectUtil.isEmpty(pickupDispatchTask)) {
            throw new SLException(WorkExceptionEnum.PICKUP_DISPATCH_TASK_NOT_FOUND);
        }
        //校验原快递id是否正确（本来无快递员id的情况除外）
        if (ObjectUtil.isNotEmpty(pickupDispatchTask.getCourierId())
                && ObjectUtil.notEqual(pickupDispatchTask.getCourierId(), originalCourierId)) {
            throw new SLException(WorkExceptionEnum.UPDATE_COURIER_ID_PARAM_ERROR);
        }
        //更改快递员id
        pickupDispatchTask.setCourierId(targetCourierId);
        // 标识已分配状态
        pickupDispatchTask.setAssignedStatus(PickupDispatchTaskAssignedStatus.DISTRIBUTED);
        //TODO 发送消息，同步更新快递员任务(ES)
        return super.updateById(pickupDispatchTask);
}

    /**
     * 取派件任务状态枚举: {@link PickupDispatchTaskStatus}
     * @param taskPickupDispatch 取派件任务信息
     * @return
     */
    @Override
    public PickupDispatchTaskEntity saveTaskPickupDispatch(PickupDispatchTaskEntity taskPickupDispatch) {
        // TODO day09 保存取派件任务

        // 1. 设置任务状态为新任务
        taskPickupDispatch.setStatus(PickupDispatchTaskStatus.NEW);
        // 2. 保存取派件任务
        boolean result = super.save(taskPickupDispatch);
        // 3. 如果保存成功 返回结果
        if (result) {
            //     TODO day12实战练习 同步快递员任务到es
            //     TODO day10 生成运单跟踪消息和快递员端取件/派件消息通知
            return taskPickupDispatch;
        }
        throw new SLException(WorkExceptionEnum.PICKUP_DISPATCH_TASK_SAVE_ERROR);
    }

    /**
     * 分页查询取派件任务
     * @param dto 查询条件
     * @return 分页结果
     */
    @Override
    public PageResponse<PickupDispatchTaskDTO> findByPage(PickupDispatchTaskPageQueryDTO dto) {
        // TODO day09 多条件分页查询取派件任务列表

        //1. 构建分页条件
        Page<PickupDispatchTaskEntity> iPage = new Page<>(dto.getPage(), dto.getPageSize());
        //2. 构建查询条件
        LambdaQueryWrapper<PickupDispatchTaskEntity> queryWrapper = Wrappers.<PickupDispatchTaskEntity>lambdaQuery()
                //2.1     任务id不为空  模糊查询条件
                .like(ObjectUtil.isNotEmpty(dto.getId()), PickupDispatchTaskEntity::getId, dto.getId())
                //2.2     订单id不为空  模糊查询条件
                .like(ObjectUtil.isNotEmpty(dto.getOrderId()), PickupDispatchTaskEntity::getOrderId, dto.getOrderId())
                //2.3     机构网点id不为空  等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getAgencyId()), PickupDispatchTaskEntity::getAgencyId, dto.getAgencyId())
                //2.4     快递员id不为空    等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getCourierId()), PickupDispatchTaskEntity::getCourierId, dto.getCourierId())
                //2.5     任务类型不为空    等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getTaskType()), PickupDispatchTaskEntity::getTaskType, dto.getTaskType())
                //2.6     任务状态不为空    等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getStatus()), PickupDispatchTaskEntity::getStatus, dto.getStatus())
                //2.7     AssignedStatusr任务分配状态不为空   等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getAssignedStatus()), PickupDispatchTaskEntity::getAssignedStatus, dto.getAssignedStatus())
                //2.8     SignStatus签收状态不为空 等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getSignStatus()), PickupDispatchTaskEntity::getSignStatus, dto.getSignStatus())
                //2.9     isDeleted删除状态不为空   等值查询条件
                .eq(ObjectUtil.isNotEmpty(dto.getIsDeleted()), PickupDispatchTaskEntity::getIsDeleted, dto.getIsDeleted())
                //2.10    getMinEstimatedEndTime getMaxEstimatedEndTime 预计完成时间不为空    区间范围查询条件
                .between(ObjectUtil.isNotEmpty(dto.getMinEstimatedEndTime()), PickupDispatchTaskEntity::getEstimatedEndTime, dto.getMinEstimatedEndTime(), dto.getMaxEstimatedEndTime())
                //2.11    getMinActualEndTime getActualEndTime 实际完成时间不为空    区间范围查询条件
                .between(ObjectUtil.isNotEmpty(dto.getMinActualEndTime()), PickupDispatchTaskEntity::getActualEndTime, dto.getMinActualEndTime(), dto.getMaxActualEndTime())
                //2.11    根据修改时间 降序排序
                .orderByDesc(PickupDispatchTaskEntity::getUpdated);
        //3. 执行分页查询
        Page<PickupDispatchTaskEntity> result = super.page(iPage, queryWrapper);
        if (ObjectUtil.isEmpty(result.getRecords())) {
            return new PageResponse<>(result);
        }
        //4. 封装返回结果
        return new PageResponse(result, PickupDispatchTaskDTO.class);
    }

    /**
     * 需要在{@link TaskPickupDispatchMapper}写sql
     * @param courierIds             快递员id列表
     * @param pickupDispatchTaskType 任务类型
     * @param date                   日期，格式：yyyy-MM-dd 或 yyyyMMdd
     * @return
     */
    @Override
    public List<CourierTaskCountDTO> findCountByCourierIds(List<Long> courierIds, PickupDispatchTaskType pickupDispatchTaskType, String date) {
        // TODO day09 查询指定快递员的任务数量

        //1. 计算一天的时间的边界  tips: 使用hutool 的 DateUtil
        DateTime dateTime = DateUtil.parse(date);
        //2. 执行SQL  tips: 下面是sql提示 需要写到TaskPickupDispatchMapper中
        // SELECT
        //        COUNT(1) `count`,
        //            courier_id
        //        FROM sl_pickup_dispatch_task t
        //        WHERE
        //            t.courier_id IN  <foreach collection="courierIds" item="courierId" open="(" close=")" separator=",">#{courierId}</foreach>
        //        AND t.created BETWEEN #{startDateTime} AND #{endDateTime}
        //        AND t.task_type = #{type}
        //        GROUP BY courier_id
        LocalDateTime startDateTime = DateUtil.beginOfDay(dateTime).toLocalDateTime();
        LocalDateTime endDateTime = DateUtil.endOfDay(dateTime).toLocalDateTime();
        return this.taskPickupDispatchMapper
                .findCountByCourierIds(courierIds, pickupDispatchTaskType.getCode(), startDateTime, endDateTime);
    }

    /**
     * 查询指定快递员当天所有的派件取件任务
     * 删除状态 {@link PickupDispatchTaskIsDeleted}
     * @param courierId 快递员id
     * @return
     */
    @Override
    public List<PickupDispatchTaskDTO> findTodayTaskByCourierId(Long courierId) {
        // TODO day09 根据快递员ID查询快递今天的任务

        // 1. 构建查询条件
        LambdaQueryWrapper<PickupDispatchTaskEntity> queryWrapper = Wrappers.<PickupDispatchTaskEntity>lambdaQuery()
                // 1.1 快递员id 等值查询
                .eq(PickupDispatchTaskEntity::getCourierId, courierId)
                // 1.2 预计开始时间 大于等于今天的开始时间  今天 00:00:00
                .ge(PickupDispatchTaskEntity::getEstimatedStartTime, LocalDateTimeUtil.beginOfDay(LocalDateTime.now()))
                // 1.3 预计开始时间 小于等于今天的结束时间  今天 23:59:59
                .le(PickupDispatchTaskEntity::getEstimatedStartTime, LocalDateTimeUtil.endOfDay(LocalDateTime.now()))
                // 1.4 isDeleted 不能是删除状态
                .eq(PickupDispatchTaskEntity::getIsDeleted, PickupDispatchTaskIsDeleted.NOT_DELETED);
        // 2. 执行查询
        // 3. 将entity集合 转为 DTO集合返回   tips: BeanUtil
        List<PickupDispatchTaskEntity> list = super.list(queryWrapper);
        return BeanUtil.copyToList(list, PickupDispatchTaskDTO.class);
    }

    /**
     *
     * @param orderId  订单id
     * @param taskType 任务类型
     * @return
     */
    @Override
    public List<PickupDispatchTaskEntity> findByOrderId(Long orderId, PickupDispatchTaskType taskType) {
        // TODO day09 查询指定订单的取派件信息

        // 1. 创建查询条件   orderId等值  taskType等值  按创建时间升序
        LambdaQueryWrapper<PickupDispatchTaskEntity> wrapper = Wrappers.<PickupDispatchTaskEntity>lambdaQuery()
                .eq(PickupDispatchTaskEntity::getOrderId, orderId)
                .eq(PickupDispatchTaskEntity::getTaskType, taskType)
                .orderByAsc(PickupDispatchTaskEntity::getCreated);
        // 2. 返回取派件任务列表
        return this.list(wrapper);
    }

    /**
     * 删除状态 {@link PickupDispatchTaskIsDeleted}
     * @param ids id列表
     * @return
     */
    @Override
    public boolean deleteByIds(List<Long> ids) {

        // TODO day09 删除取派件任务  注意： 逻辑删除

        if (CollUtil.isEmpty(ids)) {
            return false;
        }
        // 1. 遍历要删除的id列表
        List<PickupDispatchTaskEntity> list = ids.stream().map(id -> {
            // 1.1 基于每个id封装 取派件任务实体类，并设置isDeleted属性为删除状态
            PickupDispatchTaskEntity dispatchTaskEntity = new PickupDispatchTaskEntity();
            dispatchTaskEntity.setId(id);
            dispatchTaskEntity.setIsDeleted(PickupDispatchTaskIsDeleted.IS_DELETED);
            //TODO 发送消息，同步更新快递员任务（ES）
            return dispatchTaskEntity;
        }).collect(Collectors.toList());
        // 2. 批量更新取派件任务数据
        return super.updateBatchById(list);
    }

    /**
     * 今日任务分类计数
     *
     * @param courierId 快递员id
     * @param taskType  任务类型，1为取件任务，2为派件任务
     * @param status    任务状态,1新任务，2已完成，3已取消
     * @param isDeleted 是否逻辑删除
     * @return 任务数量
     */
    @Override
    public Integer todayTasksCount(Long courierId, PickupDispatchTaskType taskType, PickupDispatchTaskStatus status, PickupDispatchTaskIsDeleted isDeleted) {
        // TODO day09 今日任务数量统计

        // 1. 构建查询条件
        LambdaQueryWrapper<PickupDispatchTaskEntity> queryWrapper = Wrappers.<PickupDispatchTaskEntity>lambdaQuery()
                // 1.1 快递员id等值查询
                .eq(ObjectUtil.isNotEmpty(courierId), PickupDispatchTaskEntity::getCourierId, courierId)
                // 1.2 任务类型等值查询
                .eq(ObjectUtil.isNotEmpty(taskType), PickupDispatchTaskEntity::getTaskType, taskType)
                // 1.3 任务状态等值查询
                .eq(ObjectUtil.isNotEmpty(status), PickupDispatchTaskEntity::getStatus, status)
                // 1.4 删除状态等值查询
                .eq(ObjectUtil.isNotEmpty(isDeleted), PickupDispatchTaskEntity::getIsDeleted, isDeleted);
        // 1.5 日期区间查询条件  得到今天的开始时间，和结束时间
        LocalDateTime startTime = LocalDateTimeUtil.of(DateUtil.beginOfDay(new Date()));
        LocalDateTime endTime = LocalDateTimeUtil.of(DateUtil.endOfDay(new Date()));
        if (status == null) {
            // 1.6 如果status状态为null 查询created时间 大于等于今天开始时间 小于等于今天结束时间
            queryWrapper.between(PickupDispatchTaskEntity::getCreated, startTime, endTime);
        } else if (status == PickupDispatchTaskStatus.NEW) {
            // 1.7 如果status状态为NEW  查询EstimatedEndTime预计结束时间 大于等于今天开始时间 小于等于今天结束时间
            queryWrapper.between(PickupDispatchTaskEntity::getEstimatedEndTime, startTime, endTime);
        } else if (status == PickupDispatchTaskStatus.COMPLETED) {
            // 1.8 如果status状态为COMPLETED  查询ActualEndTime实际结束时间 大于等于今天开始时间 小于等于今天结束时间
            queryWrapper.between(PickupDispatchTaskEntity::getActualEndTime, startTime, endTime);
        } else if (status == PickupDispatchTaskStatus.CANCELLED) {
            // 1.9 如果status状态为CANCELLED  查询CancelTime任务取消时间 大于等于今天开始时间 小于等于今天结束时间
            queryWrapper.between(PickupDispatchTaskEntity::getCancelTime, startTime, endTime);
        }
        // 2. 执行查询数量方法
        // 3. 结果返回integer类型值
        return Convert.toInt(super.count(queryWrapper));
    }

    private void sendPickupDispatchTaskMsgAgain(PickupDispatchTaskEntity pickupDispatchTask){
        OrderMsg orderMsg = OrderMsg.builder()
        .agencyId(pickupDispatchTask.getAgencyId())
        .orderId(pickupDispatchTask.getOrderId())
        .created(DateUtil.current())
        .taskType(PickupDispatchTaskType.PICKUP.getCode()) //取件任务
        .mark(pickupDispatchTask.getMark())
        .estimatedEndTime(pickupDispatchTask.getEstimatedEndTime()).build();
        //发送消息（取消任务发生在取件之前，没有运单，参数直接填入null）
        this.transportOrderService.sendPickupDispatchTaskMsgToDispatch(null, orderMsg);
        }

}
