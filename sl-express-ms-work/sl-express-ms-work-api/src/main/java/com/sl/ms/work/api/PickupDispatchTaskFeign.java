package com.sl.ms.work.api;


import com.sl.ms.work.domain.dto.CourierTaskCountDTO;
import com.sl.ms.work.domain.dto.PickupDispatchTaskDTO;
import com.sl.ms.work.domain.dto.request.PickupDispatchTaskPageQueryDTO;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskIsDeleted;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskStatus;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskType;
import com.sl.transport.common.util.PageResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@FeignClient(value = "sl-express-ms-work", contextId = "pickup-dispatch", path = "pickup-dispatch-task")
public interface PickupDispatchTaskFeign {

    /**
     * 更新取派件状态，不允许 NEW 状态
     *
     * @param pickupDispatchTaskDTO 修改的数据
     * @return 是否成功
     */
    @PutMapping
    Boolean updateStatus(@RequestBody PickupDispatchTaskDTO pickupDispatchTaskDTO);

    /**
     * 改派快递员
     *
     * @param id                任务id
     * @param originalCourierId 原快递员id
     * @param targetCourierId   目标快递员id
     * @return 是否成功
     */
    @PutMapping("courier")
    Boolean updateCourierId(@RequestParam("id") Long id,
                            @RequestParam("originalCourierId") Long originalCourierId,
                            @RequestParam("targetCourierId") Long targetCourierId);

    /**
     * 获取取派件任务分页数据
     *
     * @param dto 查询条件
     * @return 取派件分页数据
     */
    @PostMapping("page")
    PageResponse<PickupDispatchTaskDTO> findByPage(@RequestBody PickupDispatchTaskPageQueryDTO dto);

    /**
     * 根据id获取取派件任务信息
     *
     * @param id 任务id
     * @return 取派件任务信息
     */
    @GetMapping("{id}")
    PickupDispatchTaskDTO findById(@PathVariable("id") Long id);

    /**
     * 根据id批量查询取派件任务信息
     *
     * @param ids 任务id列表
     * @return 任务列表
     */
    @GetMapping("ids")
    List<PickupDispatchTaskDTO> findByIds(@RequestParam("ids") List<Long> ids);

    /**
     * 根据id批量删除取派件任务信息（逻辑删除）
     *
     * @param ids 任务id列表
     * @return 是否成功
     */
    @DeleteMapping("ids")
    boolean deleteByIds(@RequestParam("ids") List<Long> ids);

    /**
     * 根据订单id查询取派件任务
     *
     * @param orderId  订单id
     * @param taskType 任务类型
     * @return 任务
     */
    @GetMapping("/orderId/{orderId}/{taskType}")
    List<PickupDispatchTaskDTO> findByOrderId(@PathVariable("orderId") Long orderId,
                                              @PathVariable("taskType") PickupDispatchTaskType taskType);

    /**
     * 按照当日快递员id列表查询每个快递员的取派件任务数
     *
     * @return 任务列表
     */
    @GetMapping("count")
    List<CourierTaskCountDTO> findCountByCourierIds();

    /**
     * 查询快递员当日的单据
     *
     * @param courierId 快递员id
     * @return 任务列表
     */
    @GetMapping("todayTasks/{courierId}")
    List<PickupDispatchTaskDTO> findTodayTasks(@PathVariable("courierId") Long courierId);

    /**
     * 今日任务分类计数
     *
     * @param courierId 快递员id
     * @param taskType  任务类型，1为取件任务，2为派件任务
     * @param status    任务状态,1新任务，2已完成，3已取消
     * @param isDeleted 是否逻辑删除
     * @return 任务数量
     */
    @GetMapping("todayTasks/count")
    Integer todayTasksCount(@RequestParam(value = "courierId") Long courierId,
                            @RequestParam(value = "taskType", required = false) PickupDispatchTaskType taskType,
                            @RequestParam(value = "status", required = false) PickupDispatchTaskStatus status,
                            @RequestParam(value = "isDeleted", required = false) PickupDispatchTaskIsDeleted isDeleted);
}
