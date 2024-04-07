package com.sl.ms.work.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.collection.ListUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.date.DateField;
import cn.hutool.core.date.DateUnit;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.io.unit.DataUnit;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.base.api.common.MQFeign;
import com.sl.ms.oms.api.CargoFeign;
import com.sl.ms.oms.api.OrderFeign;
import com.sl.ms.oms.dto.OrderCargoDTO;
import com.sl.ms.oms.dto.OrderDTO;
import com.sl.ms.oms.dto.OrderLocationDTO;
import com.sl.ms.transport.api.OrganFeign;
import com.sl.ms.transport.api.TransportLineFeign;
import com.sl.ms.work.domain.dto.TransportOrderDTO;
import com.sl.ms.work.domain.dto.request.TransportOrderQueryDTO;
import com.sl.ms.work.domain.dto.response.TransportOrderStatusCountDTO;
import com.sl.ms.work.domain.enums.WorkExceptionEnum;
import com.sl.ms.work.domain.enums.pickupDispatchtask.PickupDispatchTaskType;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderSchedulingStatus;
import com.sl.ms.work.domain.enums.transportorder.TransportOrderStatus;
import com.sl.ms.work.entity.TransportOrderEntity;
import com.sl.ms.work.entity.TransportOrderTaskEntity;
import com.sl.ms.work.mapper.TransportOrderMapper;
import com.sl.ms.work.mapper.TransportOrderTaskMapper;
import com.sl.ms.work.service.TransportOrderService;
import com.sl.ms.work.service.TransportTaskService;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.enums.IdEnum;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.common.service.IdService;
import com.sl.transport.common.util.Constant;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.common.vo.OrderMsg;
import com.sl.transport.common.vo.TransportInfoMsg;
import com.sl.transport.common.vo.TransportOrderMsg;
import com.sl.transport.common.vo.TransportOrderStatusMsg;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.domain.TransportLineNodeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Example;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import javax.json.JsonObject;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName 类名
 * @Description 类说明
 */
@Slf4j
@Service
public class TransportOrderServiceImpl extends ServiceImpl<TransportOrderMapper, TransportOrderEntity> implements TransportOrderService {
    @Resource
    private OrderFeign orderFeign;

    @Resource
    private MQFeign mqFeign;

    @Resource
    private CargoFeign cargoFeign;

    @Resource
    private TransportLineFeign transportLineFeign;

    @Resource
    private IdService idService;
    
    @Resource
    private OrganFeign organFeign;

    @Resource
    private TransportOrderTaskMapper transportOrderTaskMapper;

    @Resource
    private TransportTaskService transportTaskService;

    /**
     * 订单 转 运单
     * 运单状态: {@link TransportOrderStatus}
     * 运单调度状态: {@link TransportOrderSchedulingStatus}
     *
     * @param orderId 订单号
     * @return
     */
    @Transactional(rollbackFor = Exception.class, timeout = 120000)
    @Override
    public TransportOrderEntity orderToTransportOrder(Long orderId) {
        // TODO day06 订单 转 运单

        //幂等性校验: 根据订单id查询关联运单是否存在 存在直接返回   调用: findByOrderId
        TransportOrderEntity transportOrderEntity = this.findByOrderId(orderId);
        if (ObjectUtil.isNotEmpty(transportOrderEntity)) {
            return transportOrderEntity;
        }
        //根据订单id 远程调用订单服务查询订单数据 为空抛异常  tips: 使用orderFeign
        OrderDTO orderDTO = this.orderFeign.findById(orderId);
        if (ObjectUtil.isEmpty(orderDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_NOT_FOUND);
        }
        //根据订单id 远程调用订单服务查询订单货物数据 为空抛异常  tips: 使用cargoFeign
        OrderCargoDTO cargoDTO = this.cargoFeign.findByOrderId(orderId);
        if (ObjectUtil.isEmpty(cargoDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_CARGO_NOT_FOUND);
        }
        //根据订单id 远程调用订单服务查询订单位置信息 为空抛异常 tips: 使用orderFeign
        OrderLocationDTO orderLocationDTO = this.orderFeign.findOrderLocationByOrderId(orderId);
        if (ObjectUtil.isEmpty(orderLocationDTO)) {
            throw new SLException(WorkExceptionEnum.ORDER_LOCATION_NOT_FOUND);
        }
        //根据位置信息 获取 起始网点id 终点网点id
        Long sendAgentId = Convert.toLong(orderLocationDTO.getSendAgentId());
        Long receiveAgentId = Convert.toLong(orderLocationDTO.getReceiveAgentId());
        //声明变量: 是否需要参与调度 isDispatch (后面判断后，如果参与调度需要向调度中心发消息)
        boolean isDispatch = true;
        //声明变量: TransportLineNodeDTO运输路线 默认null
        TransportLineNodeDTO transportLineNodeDTO = null;
        // 判断: 起始网点id 终点网点id是否相等
        if (ObjectUtil.equals(sendAgentId, receiveAgentId)) {
            // 如果相等: isDispatch设置为false  (说明不需要规划路线，直接发送消息生成派件任务即可)
            isDispatch = false;
        } else {
            // 如果不相等:
            // 根据调度配置，查询运输路线,路线没查到抛出异常   tips: transportLineFeign查询
            transportLineNodeDTO = this.transportLineFeign.queryPathByDispatchMethod(sendAgentId, receiveAgentId);
            if (ObjectUtil.isEmpty(transportLineNodeDTO) || CollUtil.isEmpty(transportLineNodeDTO.getNodeList())) {
                throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
            }
        }

        //创建新的运单对象
        TransportOrderEntity transportOrder = new TransportOrderEntity();
        // 根据美团leaf服务 生成运单ID
        transportOrder.setId(this.idService.getId(IdEnum.TRANSPORT_ORDER));
        // 补全属性: orderId StartAgencyId EndAgencyId CurrentAgencyId
        transportOrder.setOrderId(orderId);//订单ID
        transportOrder.setStartAgencyId(sendAgentId);//起始网点id
        transportOrder.setEndAgencyId(receiveAgentId);//终点网点id
        transportOrder.setCurrentAgencyId(sendAgentId);//当前所在机构id
        // 判断运输路线是否为null:
        if (ObjectUtil.isEmpty(transportLineNodeDTO)) {
            // 如果为null说明 已经是终点网点:  设置下站网点就是当前发送网点id 状态：到达终端网点 调度状态: 已调度
            transportOrder.setNextAgencyId(sendAgentId);
            transportOrder.setStatus(TransportOrderStatus.ARRIVED_END);
            transportOrder.setSchedulingStatus(TransportOrderSchedulingStatus.SCHEDULED);
        } else {
            // 如果不为null说明 该运单需要转运:  设置状态为新建  调度状态: 待调度  下一站机构网点: 路线中下标为1的网点  路线: 路线JSON字符串
            transportOrder.setStatus(TransportOrderStatus.CREATED);
            transportOrder.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);
            transportOrder.setNextAgencyId(transportLineNodeDTO.getNodeList().get(1).getId());
            transportOrder.setTransportLine(JSONUtil.toJsonStr(transportLineNodeDTO));
        }
        // 补全属性: TotalWeight 货品总重量，单位kg  IsRejection 默认非拒收订单
        transportOrder.setTotalWeight(cargoDTO.getVolume());
        transportOrder.setTotalVolume(cargoDTO.getWeight());
        transportOrder.setIsRejection(false);
        //保存运单对象
        boolean result = super.save(transportOrder);
        if (result) {
            //保存成功后需要发送各类消息
            if (isDispatch) {
                // 如果需要调度
                // 发送消息到调度中心 参与运单调度 sendTransportOrderMsgToDispatch
                this.sendTransportOrderMsgToDispatch(transportOrder);
            } else {
                // 如果不需要调度
                // 发送消息 更新订单状态 sendUpdateStatusMsg
                this.sendUpdateStatusMsg(ListUtil.toList(transportOrder.getId()), TransportOrderStatus.ARRIVED_END);
                // 不需要调度，发送消息生成派件任务 sendDispatchTaskMsgToDispatch
                this.sendDispatchTaskMsgToDispatch(transportOrder);
            }
            // 发送运单创建完成消息 sendTransportOrderCreated
            String msg = TransportOrderMsg.builder()
                    .id(transportOrder.getId())
                    .orderId(transportOrder.getOrderId())
                    .created(DateUtil.current())
                    .build().toJson();
            this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED,
                    Constants.MQ.RoutingKeys.TRANSPORT_ORDER_CREATE, msg, Constants.MQ.NORMAL_DELAY);
            return transportOrder;
        }
        // 保存失败
        throw new SLException(WorkExceptionEnum.TRANSPORT_ORDER_SAVE_ERROR);
    }

    @Override
    public Page<TransportOrderEntity> findByPage(TransportOrderQueryDTO transportOrderQueryDTO) {
        // TODO day06 分页查询运单

        // 封装分页查询参数
        Page<TransportOrderEntity> iPage = Page.of(transportOrderQueryDTO.getPage(), transportOrderQueryDTO.getPageSize());
        // 设置查询条件
        LambdaQueryWrapper<TransportOrderEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        //    如果运单id不为空，根据id模糊查询
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getId()), TransportOrderEntity::getId, transportOrderQueryDTO.getId());
        //    如果订单orderId不为空，根据orderId模糊查询
        lambdaQueryWrapper.like(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getOrderId()), TransportOrderEntity::getOrderId, transportOrderQueryDTO.getOrderId());
        //    如果运单状态不为空，根据运单状态等值查询
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getStatus()), TransportOrderEntity::getStatus, transportOrderQueryDTO.getStatus());
        //    如果运单调度状态不为空，根据运单调度状态等值查询
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getSchedulingStatus()), TransportOrderEntity::getSchedulingStatus, transportOrderQueryDTO.getSchedulingStatus());
        //    如果运单起点网点ID不为空，根据起点网点ID等值查询
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getStartAgencyId()), TransportOrderEntity::getStartAgencyId, transportOrderQueryDTO.getStartAgencyId());
        //    如果运单终点网点ID不为空，根据终点网点ID等值查询
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getEndAgencyId()), TransportOrderEntity::getEndAgencyId, transportOrderQueryDTO.getEndAgencyId());
        //    如果运单当前网点ID不为空，根据当前网点ID等值查询
        lambdaQueryWrapper.eq(ObjectUtil.isNotEmpty(transportOrderQueryDTO.getCurrentAgencyId()), TransportOrderEntity::getCurrentAgencyId, transportOrderQueryDTO.getCurrentAgencyId());
        //    按照运单创建时间降序排序
        lambdaQueryWrapper.orderByDesc(TransportOrderEntity::getCreated);
        // 调用分页查询
        return super.page(iPage, lambdaQueryWrapper);
    }

    @Override
    public TransportOrderEntity findByOrderId(Long orderId) {
        // TODO day06 查询运单

        // 根据订单ID等值查询运单数据
        LambdaQueryWrapper<TransportOrderEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.eq(TransportOrderEntity::getOrderId, orderId);
        return super.getOne(lambdaQueryWrapper);
    }
    @Override
    public List<TransportOrderEntity> findByOrderIds(Long[] orderIds) {
        // TODO day06 查询运单

        // 根据多个订单ID等值查询运单数据集合
        LambdaQueryWrapper<TransportOrderEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.in(TransportOrderEntity::getOrderId, orderIds);
        return super.list(lambdaQueryWrapper);
    }

    @Override
    public List<TransportOrderEntity> findByIds(String[] ids) {
        // TODO day06 查询运单

        // 根据多个运单ID 等值查询运单集合
        LambdaQueryWrapper<TransportOrderEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.in(TransportOrderEntity::getId, ids);
        return super.list(lambdaQueryWrapper);
    }

    @Override
    public List<TransportOrderEntity> searchById(String id) {
        // TODO day06 查询运单

        // 根据输入的运单id 模糊查询运单集合
        LambdaQueryWrapper<TransportOrderEntity> lambdaQueryWrapper = new LambdaQueryWrapper<>();
        lambdaQueryWrapper.like(TransportOrderEntity::getId, id);
        return super.list(lambdaQueryWrapper);
    }

    /**
     * 修改运单状态
     * @param ids                  运单id列表
     * @param transportOrderStatus 修改的状态
     * @return
     */
    @Override
    public boolean updateStatus(List<String> ids, TransportOrderStatus transportOrderStatus) {
        // TODO day06 修改运单状态

        // 1. 校验: ids不能为空  状态不能等于CREATED
        if (ObjectUtil.isEmpty(ids)) {
            return false;
        }
        if (TransportOrderStatus.CREATED == transportOrderStatus) {
            throw new SLException(WorkExceptionEnum.TRANSPORT_ORDER_STATUS_NOT_CREATED);
        }
        // 2. 声明运单集合变量: List<TransportOrderEntity>
        List<TransportOrderEntity> transportOrderList = null;
        // 判断运单状态
        // 3 如果是拒收状态，如果是拒收需要重新查询路线，将包裹逆向回去
        if (TransportOrderStatus.REJECTED == transportOrderStatus) {
            // 3.1 根据ids查询运单列表
            transportOrderList = super.listByIds(ids);
            // 3.2 遍历运单列表
            for (TransportOrderEntity transportOrderEntity : transportOrderList) {
                // 3.2.1  设置拒收状态
                transportOrderEntity.setIsRejection(true);
                // 3.2.2  获取起始网点id 终点网点id (根据起始机构规划运输路线，这里要将起点和终点互换)
                Long startAgencyId = transportOrderEntity.getEndAgencyId();
                Long endAgencyId = transportOrderEntity.getStartAgencyId();
                // 3.2.3  声明变量 isDispatch  默认参与调度
                boolean isDispatch = true;
                // 判断起始网点id 终点网点id是否相等
                if (ObjectUtil.equals(startAgencyId, endAgencyId)) {
                    // 3.2.4  如果相等 isDispatch = false  （无需调度，直接生成派件任务）
                    isDispatch = false;
                } else {
                    // 3.2.5  如果不等:
                    // 3.2.5.1  根据调度获取运输路线 transportLineNodeDTO   tips: transportLineFeign
                    TransportLineNodeDTO transportLineNodeDTO = this.transportLineFeign.queryPathByDispatchMethod(startAgencyId, endAgencyId);
                    // 3.2.5.2  未查询到运输路线抛异常
                    if (ObjectUtil.hasEmpty(transportLineNodeDTO, transportLineNodeDTO.getNodeList())) {
                        throw new SLException(WorkExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
                    }
                    // 3.2.5.3  删除掉第一个机构，逆向回去的第一个节点就是当前所在节点
                    transportLineNodeDTO.getNodeList().remove(0);
                    // 3.2.5.4  设置运单调度状态： 待调度
                    transportOrderEntity.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);
                    // 3.2.5.5  设置当前所在机构ID
                    transportOrderEntity.setCurrentAgencyId(startAgencyId);
                    // 3.2.5.6  设置下一站网点机构ID
                    transportOrderEntity.setNextAgencyId(transportLineNodeDTO.getNodeList().get(0).getId());
                    // 3.2.5.7  获取运单中原有运输任务信息 将当前线路 追加到原有线路
                    TransportLineNodeDTO transportLineNode = JSONUtil.toBean(transportOrderEntity.getTransportLine(), TransportLineNodeDTO.class);
                    transportLineNode.getNodeList().addAll(transportLineNodeDTO.getNodeList());
                    // 3.2.5.8  合并成本 并重新设置运单线路信息
                    transportLineNode.setCost(NumberUtil.add(transportLineNode.getCost(),transportLineNodeDTO.getCost()));
                    transportOrderEntity.setTransportLine(JSONUtil.toJsonStr(transportLineNode));
                }
                // 3.2.6 设置运单为拒收状态
                transportOrderEntity.setStatus(TransportOrderStatus.REJECTED);
                // 3.2.7 判断如果需要调度  发送消息参与调度 sendTransportOrderMsgToDispatch
                if (isDispatch) {
                    this.sendTransportOrderMsgToDispatch(transportOrderEntity);
                } else {
                    // 3.2.8 判断如果不需要调度  运单状态改为到达网点:  发送消息生成派件任务 sendDispatchTaskMsgToDispatch
                    transportOrderEntity.setStatus(TransportOrderStatus.ARRIVED_END);
                    this.sendDispatchTaskMsgToDispatch(transportOrderEntity);
                }
            }
        } else {
            // 4 如果不是拒收状态，遍历根据ids列表封装成运单对象列表
            transportOrderList = ids.stream().map(id -> {
                Long nextAgencyId = this.getById(id).getNextAgencyId();
                OrganDTO organDTO = this.organFeign.queryById(nextAgencyId);
                //  构建消息实体类
                String info = CharSequenceUtil.format("快件发往【{}】", organDTO.getName());
                String transportInfoMsg = TransportInfoMsg.builder()
                        .transportOrderId(id) //运单id
                        .status("运送中") //消息状态
                        .info(info) //消息详情
                        .created(DateUtil.current()) //创建时间
                        .build().toJson();
                // 4.1 发送物流信息消息 sendTransportOrderInfoMsg      info: 快件已发往【$organId】
                this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_INFO, Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND, transportInfoMsg);
                // 4.2 封装运单对象 TransportOrderEntity 只设置 id  status用于修改即可
                TransportOrderEntity transportOrderEntity = new TransportOrderEntity();
                transportOrderEntity.setId(id);
                transportOrderEntity.setStatus(transportOrderStatus);
                return transportOrderEntity;
            }).collect(Collectors.toList());
        }
        // 5. 批量更新 运单状态数据
        boolean result = super.updateBatchById(transportOrderList);
        // 6. 发消息通知其他系统运单状态的变化 sendUpdateStatusMsg
        this.sendUpdateStatusMsg(ids, transportOrderStatus);
        return result;
    }

    @Override
    public boolean updateByTaskId(Long taskId) {
        // TODO day07 根据任务ID修改

        //1 通过运输任务查询运单id列表 为空直接结束
        List<String> transportOrderIdList = this.transportTaskService.queryTransportOrderIdListById(taskId);
        if (CollUtil.isEmpty(transportOrderIdList)) {
            return false;
        }
        //2 根据运单ids 查询运单列表
        List<TransportOrderEntity> transportOrderList = super.listByIds(transportOrderIdList);
        //3 遍历运单列表
        for (TransportOrderEntity transportOrder : transportOrderList) {
            //3.1 发送物流跟踪信息      sendTransportOrderInfoMsg      info:快件到达【$organId】
            OrganDTO organDTO = organFeign.queryById(transportOrder.getNextAgencyId());
            String info = CharSequenceUtil.format("快件到达【{}】", organDTO.getName());
            String transportInfoMsg = TransportInfoMsg.builder()
                    .transportOrderId(transportOrder.getId())//运单id
                    .status("运送中")//消息状态
                    .info(info)//消息详情
                    .created(DateUtil.current())//创建时间
                    .build().toJson();
            // 发送运单跟踪消息
            this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_INFO, Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND, transportInfoMsg);
            //3.2 将运单 CurrentAgencyId 设置为 下一站机构ID
            transportOrder.setCurrentAgencyId(transportOrder.getNextAgencyId());
            //3.3 解析完整运输路线  设置新下一站机构ID   tips: 注意运输路线格式  getTransportLine ==> 下的 nodeList为具体路线
            String transportLine = transportOrder.getTransportLine();
            JSONObject jsonObject = JSONUtil.parseObj(transportLine);
            Long nextAgencyId = 0L;
            JSONArray nodeList = jsonObject.getJSONArray("nodeList");
            //3.4 反向循环运输路线    tips: 反向循环主要是考虑到拒收的情况，路线中会存在相同的节点，始终可以查找到后面的节点
            //                             正常：A B C D E ，拒收：A B C D E D C B A
            for (int i = nodeList.size() - 1; i >= 0; i--) {
                //3.4.1   获取路线节点bid (网点机构id)
                JSONObject node = (JSONObject) nodeList.get(i);
                //3.4.2   判断当前路线节点bid 是否与当前网点id相等
                Long agencyId = node.getLong("bid");
                //3.4.3   如果相等
                if (ObjectUtil.equals(agencyId, transportOrder.getCurrentAgencyId())) {
                    //3.4.3.1    判断 下标i 是否等于 节点集合size - 1 等于说明是最后一个网点
                    if (i == nodeList.size() - 1) {
                        // nextAgencyId = 当前路线节点网点
                        nextAgencyId = agencyId;
                        // 状态为 到达终端网点状态
                        transportOrder.setStatus(TransportOrderStatus.ARRIVED_END);
                        // 发送消息更新订单状态 sendUpdateStatusMsg
                        this.sendUpdateStatusMsg(ListUtil.toList(transportOrder.getId()), TransportOrderStatus.ARRIVED_END);
                    } else {
                        //3.4.3.2    判断 下标i 是否等于 节点集合size - 1 不等于说明 i+1 就是下一站网点
                        // nextAgencyId = 下标(i + 1)网点.bid
                        nextAgencyId = ((JSONObject) nodeList.get(i + 1)).getLong("bid");
                        //设置运单状态为待调度
                        transportOrder.setSchedulingStatus(TransportOrderSchedulingStatus.TO_BE_SCHEDULED);
                    }
                    // 设置运单状态为待调度状态 break
                    break;
                }
            }
            //3.5 设置运单下一站网点id nextAgencyId
            transportOrder.setNextAgencyId(nextAgencyId);
            //3.6 如果运单没有到达终点，需要发送消息到运单调度中心等待调度 sendTransportOrderMsgToDispatch
            if (ObjectUtil.notEqual(transportOrder.getStatus(), TransportOrderStatus.ARRIVED_END)) {
                this.sendTransportOrderMsgToDispatch(transportOrder);
            } else {
                //3.7 如果已经到达最终网点，需要发送消息，进行分配快递员作业 sendDispatchTaskMsgToDispatch
                //发送消息生成派件任务
                this.sendDispatchTaskMsgToDispatch(transportOrder);
            }
        }
        //4. 批量更新运单信息
        return super.updateBatchById(transportOrderList);
    }

    /**
     * 运单状态枚举: {@link TransportOrderStatus}
     * @return
     */
    @Override
    public List<TransportOrderStatusCountDTO> findStatusCount() {
        // TODO day06 查询运单

        // 统计不同状态的 运单数量
        // 获取所有枚举状态，封装List<TransportOrderStatusCountDTO>集合，每个状态count值都是0
        List<TransportOrderStatusCountDTO> statusCountList = Arrays.stream(TransportOrderStatus.values())
                .map(transportOrderStatus -> TransportOrderStatusCountDTO.builder()
                    .status(transportOrderStatus)
                    .statusCode(transportOrderStatus.getCode())
                    .count(0L).build())
                .collect(Collectors.toList());

        // [新建: 0,已装车: 0,运输中: 0,到达终端网点: 0,拒收: 0]
        // 执行sql: SELECT `status` AS statusCode,count(1) AS count FROM sl_transport_order GROUP BY `status`
        List<TransportOrderStatusCountDTO> statusCount = super.baseMapper.findStatusCount();
        // [新建: 5, 运输中: 10,到达终端网点: 3]
        // 根据查询到的 状态数量结果，修改上面完整集合中的状态数量
        // [新建: 5,已装车: 10,运输中: 0,到达终端网点: 3,拒收: 0]
        for (TransportOrderStatusCountDTO transportOrderStatusCountDTO : statusCountList) {
            for (TransportOrderStatusCountDTO countDTO : statusCount) {
                if (ObjectUtil.equals(transportOrderStatusCountDTO.getStatusCode(), countDTO.getStatusCode())) {
                    transportOrderStatusCountDTO.setCount(countDTO.getCount());
                    break;
                }
            }
        }
        return statusCountList;
    }

    /**
     * 根据运输任务id分页查询运单信息
     *
     * @param page             页码
     * @param pageSize         页面大小
     * @param taskId           运输任务id
     * @param transportOrderId 运单id
     * @return 运单对象分页数据
     */
    @Override
    public PageResponse<TransportOrderDTO> pageQueryByTaskId(Integer page, Integer pageSize, String taskId, String transportOrderId) {
        // TODO day07 根据运输任务id 分页查询运单信息

        //1. 构建分页查询条件
        Page<TransportOrderTaskEntity> transportOrderTaskPage = new Page<>(page, pageSize);
        //2. 构建查询条件:
        LambdaQueryWrapper<TransportOrderTaskEntity> queryWrapper = new LambdaQueryWrapper<>();
        // 2.1 如果taskId不为空，等值查询taskId
        queryWrapper.eq(ObjectUtil.isNotEmpty(taskId), TransportOrderTaskEntity::getTransportTaskId, taskId)
                // 2.2 如果transportOrderId不为空，模糊查询transportOrderId
                .like(ObjectUtil.isNotEmpty(transportOrderId), TransportOrderTaskEntity::getTransportOrderId, transportOrderId)
                // 2.3 按照运单创建时间降序排序
                .orderByDesc(TransportOrderTaskEntity::getCreated);
        //3. 执行分页查询得到运输任务订单关联数据   tips: 调用transportOrderTaskMapper分页查询
        Page<TransportOrderTaskEntity> pageResult = transportOrderTaskMapper.selectPage(transportOrderTaskPage, queryWrapper);
        //4. 如果结果为空 直接返回
        if (ObjectUtil.isEmpty(pageResult.getRecords())) {
            return new PageResponse<>(pageResult);
        }
        //5. 根据关联运单ids查询到运单数据   注意要将运单entity ==> 运单DTO集合
        List<TransportOrderDTO> transportOrderDTOList = pageResult.getRecords().stream().map(x -> {
            TransportOrderEntity transportOrderEntity = this.getById(x.getTransportOrderId());
            return BeanUtil.toBean(transportOrderEntity, TransportOrderDTO.class);
        }).collect(Collectors.toList());
        //6. 封装分页结果返回
        return PageResponse.<TransportOrderDTO>builder()
                .page(page)
                .pageSize(pageSize)
                .pages(pageResult.getPages())
                .counts(pageResult.getTotal())
                .items(transportOrderDTOList)
                .build();
    }

    /**
     * 发送运单消息到调度中，参与调度
     */
    private void sendTransportOrderMsgToDispatch(TransportOrderEntity transportOrder) {
        Map<Object, Object> msg = MapUtil.builder()
                .put("transportOrderId", transportOrder.getId())
                .put("currentAgencyId", transportOrder.getCurrentAgencyId())
                .put("nextAgencyId", transportOrder.getNextAgencyId())
                .put("totalWeight", transportOrder.getTotalWeight())
                .put("totalVolume", transportOrder.getTotalVolume())
                .put("created", System.currentTimeMillis()).build();
        String jsonMsg = JSONUtil.toJsonStr(msg);
        //发送消息，延迟5秒，确保本地事务已经提交，可以查询到数据
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED,
                Constants.MQ.RoutingKeys.JOIN_DISPATCH, jsonMsg, Constants.MQ.LOW_DELAY);
    }

    /**
     * 发送生成取派件任务的消息
     *
     * @param transportOrder 运单对象
     */
    private void sendDispatchTaskMsgToDispatch(TransportOrderEntity transportOrder) {
        //预计完成时间，如果是中午12点到的快递，当天22点前，否则，第二天22点前
        int offset = 0;
        if (LocalDateTime.now().getHour() >= 12) {
            offset = 1;
        }
        LocalDateTime estimatedEndTime = DateUtil.offsetDay(new Date(), offset)
                .setField(DateField.HOUR_OF_DAY, 22)
                .setField(DateField.MINUTE, 0)
                .setField(DateField.SECOND, 0)
                .setField(DateField.MILLISECOND, 0).toLocalDateTime();
        //发送分配快递员派件任务的消息
        OrderMsg orderMsg = OrderMsg.builder()
                .agencyId(transportOrder.getCurrentAgencyId())
                .orderId(transportOrder.getOrderId())
                .created(DateUtil.current())
                .taskType(PickupDispatchTaskType.DISPATCH.getCode()) //派件任务
                .mark("系统提示：派件前请与收件人电话联系.")
                .estimatedEndTime(estimatedEndTime).build();
        //发送消息
        this.sendPickupDispatchTaskMsgToDispatch(transportOrder, orderMsg);
    }

    /**
     * 发送消息到调度中心，用于生成取派件任务
     * @param transportOrder 运单
     * @param orderMsg       消息内容
     */
    @Override
    public void sendPickupDispatchTaskMsgToDispatch(TransportOrderEntity transportOrder, OrderMsg orderMsg) {
        //查询订单对应的位置信息
        OrderLocationDTO orderLocationDTO = this.orderFeign.findOrderLocationByOrderId(orderMsg.getOrderId());
        //(1)运单为空：取件任务取消，取消原因为返回网点；重新调度位置取寄件人位置
        //(2)运单不为空：生成的是派件任务，需要根据拒收状态判断位置是寄件人还是收件人
        // 拒收：寄件人  其他：收件人
        String location;
        if (ObjectUtil.isEmpty(transportOrder)) {
            location = orderLocationDTO.getSendLocation();
        } else {
            location = transportOrder.getIsRejection() ? orderLocationDTO.getSendLocation() : orderLocationDTO.getReceiveLocation();
        }
        Double[] coordinate = Convert.convert(Double[].class, StrUtil.split(location, ","));
        Double longitude = coordinate[0];
        Double latitude = coordinate[1];
        //设置消息中的位置信息
        orderMsg.setLongitude(longitude);
        orderMsg.setLatitude(latitude);
        //发送消息,用于生成取派件任务
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.ORDER_DELAYED, Constants.MQ.RoutingKeys.ORDER_CREATE,
                orderMsg.toJson(), Constants.MQ.NORMAL_DELAY);
    }

    /**
     * 消息会发送到订单服务  根据运单状态修改订单状态
     * @param ids
     * @param transportOrderStatus
     */
    private void sendUpdateStatusMsg(List<String> ids, TransportOrderStatus transportOrderStatus) {
        // 订单服务 监听此消息  用于修改order状态
        String msg = TransportOrderStatusMsg.builder()
                .idList(ids)
                .statusName(transportOrderStatus.name())
                .statusCode(transportOrderStatus.getCode())
                .build().toJson();
        //将状态名称写入到路由key中，方便消费方选择性的接收消息
        String routingKey = Constants.MQ.RoutingKeys.TRANSPORT_ORDER_UPDATE_STATUS_PREFIX + transportOrderStatus.name();
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED, routingKey, msg, Constants.MQ.LOW_DELAY);
    }

    private void sendTransportOrderCreated(TransportOrderEntity transportOrder) {
        // 后续search搜索服务关注此消息
        String msg = TransportOrderMsg.builder()
                .id(transportOrder.getId())
                .orderId(transportOrder.getOrderId())
                .created(DateUtil.current())
                .build().toJson();
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_ORDER_DELAYED,
                Constants.MQ.RoutingKeys.TRANSPORT_ORDER_CREATE, msg, Constants.MQ.NORMAL_DELAY);
    }

    private void sendTransportOrderInfoMsg(String id,String info){
        this.sendTransportOrderInfoMsg(this.getById(id),info);
    }
    private void sendTransportOrderInfoMsg(TransportOrderEntity transportOrder,String info){
        //获取将发往的目的地机构
        Long nextAgencyId = transportOrder.getNextAgencyId();
        //构建消息实体类
        String transportInfoMsg = TransportInfoMsg.builder()
                .transportOrderId(transportOrder.getId())//运单id
                .status("运送中")//消息状态
                .info(info)//消息详情
                .organId(nextAgencyId)
                .created(DateUtil.current())//创建时间
                .build().toJson();
        //发送运单跟踪消息
        this.mqFeign.sendMsg(Constants.MQ.Exchanges.TRANSPORT_INFO, Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND, transportInfoMsg);
    }
}
