package com.sl.transport.service.impl;
import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.google.gson.JsonObject;
import com.itheima.em.sdk.EagleMapTemplate;
import com.itheima.em.sdk.enums.ProviderEnum;
import com.itheima.em.sdk.vo.Coordinate;
import com.sl.transport.common.enums.DispatchMethodEnum;
import com.sl.transport.common.exception.SLException;
import com.sl.transport.common.util.PageResponse;
import com.sl.transport.domain.DispatchConfigurationDTO;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.domain.TransportLineNodeDTO;
import com.sl.transport.domain.TransportLineSearchDTO;
import com.sl.transport.entity.line.TransportLine;
import com.sl.transport.entity.node.AgencyEntity;
import com.sl.transport.entity.node.BaseEntity;
import com.sl.transport.entity.node.OLTEntity;
import com.sl.transport.entity.node.TLTEntity;
import com.sl.transport.enums.ExceptionEnum;
import com.sl.transport.enums.TransportLineEnum;
import com.sl.transport.repository.TransportLineRepository;
import com.sl.transport.service.DispatchConfigurationService;
import com.sl.transport.service.OrganService;
import com.sl.transport.service.TransportLineService;
import org.springframework.retry.backoff.Sleeper;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
/**
 * 路线相关业务
 */
@Service
public class TransportLineServiceImpl implements TransportLineService {

    @Resource
    private TransportLineRepository transportLineRepository;
    @Resource
    private EagleMapTemplate eagleMapTemplate;
    @Resource
    private OrganService organService;
    @Resource
    private DispatchConfigurationService dispatchConfigurationService;



    /**
     * 新增路线业务规则：干线：起点终点无顺序，支线：起点必须是二级转运中心，接驳路线：起点必须是网点
     * 路线类型枚举: {@link TransportLineEnum}
     * @param transportLine 路线数据
     * @return
     */
    @Override
    public Boolean createLine(TransportLine transportLine) {
        // TODO day05 新增路线
        // 校验路线类型不能为空 tips: 根据type获取路线枚举在判断哦
        TransportLineEnum transportLineEnum = TransportLineEnum.codeOf(transportLine.getType());
        if (null == transportLineEnum) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_TYPE_ERROR);
        }
        // 定义 出发路线实体 到达路线实体 类型:BaseEntity
        BaseEntity firstNode;
        BaseEntity secondNode;
        // 判断路线类型枚举，根据不同类型 baseEntity实现类不同
        switch (transportLineEnum) {
            case TRUNK_LINE:
                //      如果是 干线，  则出发路线实体 到达路线实体 都为 OLTEntity
                firstNode = OLTEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = OLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            case BRANCH_LINE:
                //      如果是 支线，  则出发路线实体为二级分拣中心TLTEntity  到达路线实体为一级转运中心OLTEntity
                firstNode = TLTEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = OLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            case CONNECT_LINE:
                //      如果是 接驳路线  则出发路线实体为三级营业部AgencyEntity 到达路线实体为二级分拣中心TLTEntity
                firstNode = AgencyEntity.builder().bid(transportLine.getStartOrganId()).build();
                secondNode = TLTEntity.builder().bid(transportLine.getEndOrganId()).build();
                break;
            default:
                //      如果都不是 抛出路线类型错误异常
                throw new SLException(ExceptionEnum.TRANSPORT_LINE_TYPE_ERROR);
        }
        if (ObjectUtil.hasEmpty(firstNode, secondNode)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        // 判断路线是否已经存在,存在抛以存在异常  tips: transportLineRepository根据两个节点查询count路线数量即可
        Long count = transportLineRepository.queryCount(firstNode, secondNode);
        if (count > 0) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_ALREADY_EXISTS);
        }
        // 补全路线属性: id=null created updated当前时间
        transportLine.setId(null);
        transportLine.setCreated(System.currentTimeMillis());
        transportLine.setUpdated(transportLine.getCreated());
        // 补充其它信息  tips: 调用infoFromMap补全
        this.infoFromMap(firstNode, secondNode, transportLine);
        // 调用创建路线方法 tips: transportLineRepository创建方法
        count = this.transportLineRepository.create(firstNode, secondNode, transportLine);
        return count > 0;
    }
    /**
     * 通过地图查询距离、时间，计算成本
     * @param firstNode     开始节点
     * @param secondNode    结束节点
     * @param transportLine 路线对象
     */
    private void infoFromMap(BaseEntity firstNode, BaseEntity secondNode, TransportLine transportLine) {
        // 根据Bid查询 发起机构节点数据
        OrganDTO startOrgan = this.organService.findByBid(firstNode.getBid());
        // 校验： 不能为空， 经纬度不能为空  如果为空  抛异常提示请先完善机构信息
        if (ObjectUtil.hasEmpty(startOrgan, startOrgan.getLatitude(), startOrgan.getLongitude())) {
            throw new SLException("请先完善机构信息");
        }
        // 根据Bid查询 到达机构节点数据
        OrganDTO endOrgan = this.organService.findByBid(secondNode.getBid());
        // 校验： 不能为空， 经纬度不能为空  如果为空  抛异常提示请先完善机构信息
        if (ObjectUtil.hasEmpty(endOrgan, endOrgan.getLatitude(), endOrgan.getLongitude())) {
            throw new SLException("请先完善机构信息");
        }
        // 查询高德地图行驶路线方法 tips: eagleMapTemplate.opsForDirection().driving() 行驶路线
                             //   tips: Coordinate坐标点参数
                            //    tips: 设置高德地图参数，默认是不返回预计耗时的，需要额外设置参数
                                    //Map<String, Object> param = MapUtil.<String, Object>builder().put("show_fields", "cost").build();
        // 得到驾驶路线信息，如果为空return, 不为空转JSON对象  tips: 使用JSONUtil工具类
        Coordinate origin = new Coordinate(startOrgan.getLongitude(), startOrgan.getLatitude());
        Coordinate destination = new Coordinate(startOrgan.getLongitude(), startOrgan.getLatitude());
        Map<String, Object> param = MapUtil.<String, Object>builder().put("show_fields", "cost").build();
        String driving = this.eagleMapTemplate.opsForDirection().driving(ProviderEnum.AMAP, origin, destination, param);
        if (StrUtil.isEmpty(driving)) {
            return;
        }
        JSONObject jsonObject = JSONUtil.parseObj(driving);
        // 获取预计消耗时间，单位：秒 ，设置到路线中   tips: route.paths[0].cost.duration
        Long duration = Convert.toLong(jsonObject.getByPath("route.paths[0].cost.duration"), -1L);
        transportLine.setTime(duration);
        // 获取路线距离，单位：米, 设置到路线中  tips: route.paths[0].distance
        Double distance = Convert.toDouble(jsonObject.getByPath("route.paths[0].distance"),-1D);
        transportLine.setDistance(distance);
        // 获取路线成本，单位: 元  tips: route.taxi_cost
        // 说明: 这里按照高德地图的预计打车费用作为成本计算，同一标准在计算路线时是可行的，但是不能作为真实的成本进行利润计算
        Double cost = Convert.toDouble(jsonObject.getByPath("route.taxi_cost"), -1D);
        transportLine.setCost(cost);
    }
    @Override
    public Boolean updateLine(TransportLine transportLine) {
        // TODO day05 修改路线
        // 先根据路线ID查询路线，不存在抛出异常
        TransportLine transportLineData = this.queryById(transportLine.getId());
        if (null == transportLineData) {
            throw new SLException(ExceptionEnum.TRANSPORT_LINE_NOT_FOUND);
        }
        // 拷贝数据    tips: 基于hutool BeanUtil拷贝
        // 忽略拷贝字段: null字段  type startOrganId startOrganName endOrganId  endOrganName 这些字段不允许修改
        BeanUtil.copyProperties(transportLine, transportLineData, CopyOptions.create().setIgnoreNullValue(true)
                .setIgnoreProperties("type", "startOrganId", "startOrganName", "endOrganId", "endOrganName"));
        // 设置updated修改时间
        transportLineData.setUpdated(System.currentTimeMillis());
        // 修改路线
        Long count = this.transportLineRepository.update(transportLineData);
        return count > 0;
    }
    @Override
    public Boolean deleteLine(Long id) {
        // TODO day05 删除路线
        return this.transportLineRepository.remove(id) > 0;
    }
    @Override
    public PageResponse<TransportLine> queryPageList(TransportLineSearchDTO transportLineSearchDTO) {
        return this.transportLineRepository.queryPageList(transportLineSearchDTO);
    }
    @Override
    public TransportLineNodeDTO queryShortestPath(Long startId, Long endId) {
        // TODO day05 距离优先 查询最短转运距离路线

        // 封装起止网点查询数据
        AgencyEntity start = AgencyEntity.builder().bid(startId).build();
        AgencyEntity end = AgencyEntity.builder().bid(endId).build();
        // 校验都不能为空
        if (ObjectUtil.hasEmpty(start, end)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        // 查询最短路线
        // 注意： 一定要阅读Dao层实现代码  理解逻辑
        return this.transportLineRepository.findShortestPath(start, end);
    }
    @Override
    public TransportLineNodeDTO findLowestPath(Long startId, Long endId) {
        // TODO day05 成本优先 查询成本最少路线

        // 封装起止网点查询数据
        AgencyEntity start = AgencyEntity.builder().bid(startId).build();
        AgencyEntity end = AgencyEntity.builder().bid(endId).build();
        // 校验都不能为空
        if (ObjectUtil.hasEmpty(start, end)) {
            throw new SLException(ExceptionEnum.START_END_ORGAN_NOT_FOUND);
        }
        // 查询成本最低路线
        // 注意： 一定要阅读Dao层实现代码  理解逻辑
        List<TransportLineNodeDTO> pathList = this.transportLineRepository.findPathList(start, end, 10, 1);
        if (null != pathList) {
            return pathList.get(0);
        }
        return null;
    }
    /**
     * 根据调度策略查询路线
     *
     * @param startId 开始网点id
     * @param endId   结束网点id
     * @return 路线
     */
    @Override
    public TransportLineNodeDTO queryPathByDispatchMethod(Long startId, Long endId) {
        //获取系统中 调度方式配置
        DispatchConfigurationDTO configuration = this.dispatchConfigurationService.findConfiguration();
        int method = configuration.getDispatchMethod();
        //调度方式，1转运次数最少，2成本最低
        if (ObjectUtil.equal(DispatchMethodEnum.SHORTEST_PATH.getCode(), method)) {
            return this.queryShortestPath(startId, endId);
        } else {
            return this.findLowestPath(startId, endId);
        }
    }
    @Override
    public List<TransportLine> queryByIds(Long... ids) {
        return this.transportLineRepository.queryByIds(ids);
    }
    @Override
    public TransportLine queryById(Long id) {
        return this.transportLineRepository.queryById(id);
    }
}