package com.sl.ms.scope.service.impl;
import cn.hutool.core.util.ObjectUtil;
import com.itheima.em.sdk.EagleMapTemplate;
import com.itheima.em.sdk.enums.ProviderEnum;
import com.itheima.em.sdk.vo.Coordinate;
import com.itheima.em.sdk.vo.GeoResult;
import com.sl.ms.scope.entity.ServiceScopeEntity;
import com.sl.ms.scope.enums.ServiceTypeEnum;
import com.sl.ms.scope.service.ScopeService;
import lombok.extern.slf4j.Slf4j;
import org.bson.types.ObjectId;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.geo.GeoJsonPoint;
import org.springframework.data.mongodb.core.geo.GeoJsonPolygon;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;
import javax.annotation.Resource;
import java.util.List;
@Slf4j
@Service
public class ScopeServiceImpl implements ScopeService {
    @Resource
    private MongoTemplate mongoTemplate;
    @Resource
    private EagleMapTemplate eagleMapTemplate;

    /**
     * 保存或修改 作业范围
     * @param bid     业务id
     * @param type    类型
     * @param polygon 多边形坐标点
     * @return
     */
    @Override
    public Boolean saveOrUpdate(Long bid, ServiceTypeEnum type, GeoJsonPolygon polygon) {
        // TODO day08 新增作业范围

        // 1. 构建mongo查询条件  bid    type
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode()));
        // 2. 根据条件查询对应作业范围
        ServiceScopeEntity serviceScopeEntity = this.mongoTemplate.findOne(query, ServiceScopeEntity.class);
        // 3. 如果作业范围为空 ==> 新建作业范围保存
        if (ObjectUtil.isEmpty(serviceScopeEntity)) {
            //新增
            serviceScopeEntity = new ServiceScopeEntity();
            serviceScopeEntity.setBid(bid);
            serviceScopeEntity.setType(type.getCode());
            serviceScopeEntity.setPolygon(polygon);
            serviceScopeEntity.setCreated(System.currentTimeMillis());
            serviceScopeEntity.setUpdated(serviceScopeEntity.getCreated());
        } else {
            // 4. 如果作业范围存在 ==> 修改多边形字段 和 updated时间
            //更新
            serviceScopeEntity.setPolygon(polygon);
            serviceScopeEntity.setUpdated(System.currentTimeMillis());
        }
        // 5. 返回结果
        try {
            this.mongoTemplate.save(serviceScopeEntity);
            return true;
        } catch (Exception e) {
            log.error("新增/更新服务范围数据失败！ bid = {}, type = {}, points = {}", bid, type, polygon.getPoints(), e);
        }
        return false;
    }
    @Override
    public Boolean delete(String id) {
        // TODO day08 删除作业范围
        Query query = Query.query(Criteria.where("id").is(new ObjectId(id))); //构造查询条件
        return this.mongoTemplate.remove(query, ServiceScopeEntity.class).getDeletedCount() > 0;
    }
    @Override
    public Boolean delete(Long bid, ServiceTypeEnum type) {
        // TODO day08 删除作业范围
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode())); //构造查询条件
        return this.mongoTemplate.remove(query, ServiceScopeEntity.class).getDeletedCount() > 0;
    }
    @Override
    public ServiceScopeEntity queryById(String id) {
        // TODO day08 根据id查询作业范围
        return this.mongoTemplate.findById(new ObjectId(id), ServiceScopeEntity.class);
    }
    @Override
    public ServiceScopeEntity queryByBidAndType(Long bid, ServiceTypeEnum type) {
        // TODO day08 根据bid和类型查询作业范围
        Query query = Query.query(Criteria.where("bid").is(bid).and("type").is(type.getCode())); //构造查询条件
        return this.mongoTemplate.findOne(query, ServiceScopeEntity.class);
    }
    @Override
    public List<ServiceScopeEntity> queryListByPoint(ServiceTypeEnum type, GeoJsonPoint point) {
        // TODO day08 根据类型 和 坐标点查询有交集的作业范围     tips: intersects 查询传入坐标点和mongo库中多边形相交的数据
        Query query = Query.query(Criteria.where("polygon").intersects(point)
                .and("type").is(type.getCode()));
        return this.mongoTemplate.find(query, ServiceScopeEntity.class);
    }
    @Override
    public List<ServiceScopeEntity> queryListByPoint(ServiceTypeEnum type, String address) {
        // TODO day08 根据类型 和 详细地址     tips: eagleMapTemplate 查询坐标点
        //根据详细地址查询坐标
        GeoResult geoResult = this.eagleMapTemplate.opsForBase().geoCode(ProviderEnum.AMAP, address, null);
        Coordinate coordinate = geoResult.getLocation();
        return this.queryListByPoint(type, new GeoJsonPoint(coordinate.getLongitude(), coordinate.getLatitude()));
    }
}