package com.sl.ms.carriage.service.impl;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.*;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.sl.ms.base.api.common.AreaFeign;
import com.sl.ms.carriage.domain.constant.CarriageConstant;
import com.sl.ms.carriage.domain.dto.CarriageDTO;
import com.sl.ms.carriage.domain.dto.WaybillDTO;
import com.sl.ms.carriage.domain.enums.EconomicRegionEnum;
import com.sl.ms.carriage.entity.CarriageEntity;
import com.sl.ms.carriage.enums.CarriageExceptionEnum;
import com.sl.ms.carriage.mapper.CarriageMapper;
import com.sl.ms.carriage.service.CarriageService;
import com.sl.ms.carriage.utils.CarriageUtils;
import com.sl.transport.common.exception.SLException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @ClassName 类名
 * @Description 类说明
 */
@Slf4j
@Service
public class CarriageServiceImpl extends ServiceImpl<CarriageMapper, CarriageEntity> implements CarriageService {

    @Resource
    private AreaFeign areaFeign;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 模板缓存key
    private static final String CACHE_KEY = "CARRIAGE_CACHE";

    /**
     * 流程说明：
     * ● 根据传入的CarriageDTO对象参数进行查询模板，判断模板是否存在，如果不存在直接落库
     * ● 如果存在，需要进一步的判断是否为经济区互寄，如果不是，说明模板重复，不能落库
     * ● 如果是经济区互寄，再进一步的判断是否有重复的城市，如果是，模板重复，不能落库
     * ● 如果不重复，落库，响应返回
     * 模板为什么不能重复？
     * 因为运费的计算是通过模板进行的，如果存在多个模板，该基于哪个模板计算呢？所以模板是不能重复的。
     * @param carriageDto 新增/修改运费对象
     * @return
     *
     * 模板类型常量 {@link com.sl.ms.carriage.domain.constant.CarriageConstant}
     *
     */
    @Override
    public CarriageDTO saveOrUpdate(CarriageDTO carriageDto) {
        // TODO day02 保存或修改 运费模板

        log.info("新增运费模板 ---> {}", carriageDto);
        // 校验运费模板是否存在，如果不存在直接插入 (查询条件： 模板类型  运输类型   如果是修改排除当前id)
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.<CarriageEntity>lambdaQuery()
                .eq(CarriageEntity::getTemplateType, carriageDto.getTemplateType())
                .eq(CarriageEntity::getTransportType, carriageDto.getTransportType())
                // 如果是修改操作，需要查询出其他的模板数据，用于是否重复判定（如果不去除自己的话肯定重复）
                .ne(ObjectUtil.isNotEmpty(carriageDto.getId()), CarriageEntity::getId, carriageDto.getId());
        //查询数据库
        List<CarriageEntity> carriageList = super.list(queryWrapper);
        if (CollUtil.isEmpty(carriageList)) {
            // 如果没有重复的模板，可以直接插入或更新操作 (DTO 转 entity 保存成功 entity 转 DTO)
            return this.saveOrUpdateCarriage(carriageDto);
        }
        // 如果存在重复模板，需要判断此次插入的是否为经济区互寄，非经济区互寄是不可以重复的
        // 校验是否为经济区
        if (ObjectUtil.notEqual(carriageDto.getTemplateType(), CarriageConstant.ECONOMIC_ZONE)) {
            // 非经济区，模板重复
            throw new SLException(CarriageExceptionEnum.NOT_ECONOMIC_ZONE_REPEAT);
        }
        // 如果是经济区互寄类型，需进一步判断关联城市是否重复，通过集合取交集判断是否重复
        List<String> allList = carriageList.stream().map(CarriageEntity::getAssociatedCity)
                .map(associatedCity -> StrUtil.splitToArray(associatedCity, ","))
                .flatMap(Arrays::stream)
                .collect(Collectors.toList());
        // 取交集
        Collection<String> intersection = CollUtil.intersection(allList, carriageDto.getAssociatedCityList());
        // 如果没有重复，可以新增或更新 (DTO 转 entity 保存成功 entity 转 DTO)
        if (CollUtil.isNotEmpty(intersection)) {
            // 重复
            throw new SLException(CarriageExceptionEnum.ECONOMIC_ZONE_CITY_REPEAT);
        }
        // 没有重复，可以新增或更新
        return this.saveOrUpdateCarriage(carriageDto);
    }

    /**
     * 新增或更数据到数据库
     *
     * @param carriageDTO 传入的DTO数据
     * @return 新的DTO数据
     */
    private CarriageDTO saveOrUpdateCarriage(CarriageDTO carriageDTO) {
        // 将DTO转成Entity
        CarriageEntity carriageEntity = CarriageUtils.toEntity(carriageDTO);
        super.saveOrUpdate(carriageEntity);
        // 转化成DTO返回
        return CarriageUtils.toDTO(carriageEntity);
    }

    @Override
    public List<CarriageDTO> findAll() {
        // TODO day02 查询运费模板

        // 构造查询条件，按创建时间倒序
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.<CarriageEntity>lambdaQuery()
                .orderByDesc(CarriageEntity::getCreated);
        // 查询数据库
        List<CarriageEntity> list = super.list(queryWrapper);
        // 将结果转换为DTO类型  使用CarriageUtils工具类
        return list.stream().map(CarriageUtils::toDTO).collect(Collectors.toList());
    }

    /**
     * ● 运费模板优先级：同城>省内>经济区互寄>跨省
     * ● 将体积转化成重量，与重量比较，取大值
     * @param waybillDTO 运费计算对象
     * @return
     *
     *  //TODO day02练习 模板缓存  推荐hash结构   大key自定义   小key:发件城市id_收件城市id value: 模板数据
     */
    @Override
    public CarriageDTO compute(WaybillDTO waybillDTO) {
        //TODO day02 计算运费
        CarriageEntity carriage;
        String redisHashKey = StrUtil.format("{}_{}", waybillDTO.getSenderCityId(), waybillDTO.getReceiverCityId());
        Object cacheData = this.stringRedisTemplate.opsForHash().get(CACHE_KEY, redisHashKey);
        if (ObjectUtil.isNotEmpty(cacheData)) {
            carriage = JSONUtil.toBean(Convert.toStr(cacheData), CarriageEntity.class);
        } else {
            // 根据参数查找运费模板 调用findCarriage方法
            carriage = this.findCarriage(waybillDTO);
            // 写到缓存中
            this.stringRedisTemplate.opsForHash().put(CACHE_KEY, redisHashKey, JSONUtil.toJsonStr(carriage));
        }
        // 计算重量，最小重量为1kg 调用getComputeWeight方法
        double computeWeight = this.getComputeWeight(waybillDTO, carriage);
        // 计算运费  运费 = 首重价格 + (实际重量 - 1) * 续重架构
        double expense = carriage.getFirstWeight() + ((computeWeight - 1) * carriage.getContinuousWeight());
        // 结果保留一位小数
        expense = NumberUtil.round(expense, 1).doubleValue();
        // 封装运费和计算重量到 CarriageDTO，并返回
        CarriageDTO carriageDTO = CarriageUtils.toDTO(carriage);
        carriageDTO.setExpense(expense);
        carriageDTO.setComputeWeight(computeWeight);
        return carriageDTO;
    }
    /**
     * 根据体积参数与实际重量计算计费重量
     *
     * @param waybillDTO 运费计算对象
     * @param carriage   运费模板
     * @return 计费重量
     */
    private double getComputeWeight(WaybillDTO waybillDTO, CarriageEntity carriage) {
        // 计算体积，如果传入体积则不需要计算
         Integer volume = waybillDTO.getVolume();
         if (ObjectUtil.isEmpty(volume)) {
             try {
                 // 长 * 宽 * 高计算体积
                 volume = waybillDTO.getMeasureLong() * waybillDTO.getMeasureWidth() * waybillDTO.getMeasureHigh();
             } catch (Exception e) {
                 // 计算出错设置体积为0
                 volume = 0;
             }
         }
        // 计算体积重量  = 体积 / 轻抛系数  tips: 使用NumberUtil工具类计算 保留一位小数
        BigDecimal volumeWeight = NumberUtil.div(volume, carriage.getLightThrowingCoefficient(), 1);
        // 重量取大值 = 体积重量 和 实际重量 tips: 使用NumberUtil工具类计算   保留一位小数
        double computeWeight = NumberUtil.max(volumeWeight.doubleValue(), NumberUtil.round(waybillDTO.getWeight(), 1).doubleValue());
        // 计算续重，规则：不满1kg，按1kg计费；
        if (computeWeight <= 1) {
            return 1;
        }
        // 10kg以下续重以0.1kg计量保留1位小数；
        if (computeWeight <= 10) {
            return computeWeight;
        }
        // 100kg以上四舍五入取整  举例：108.4kg按照108kg收费 108.5kg按照109kg收费
        // tips: 使用NumberUtil工具类计算
        if (computeWeight >= 100) {
            return NumberUtil.round(computeWeight, 0).doubleValue();
        }
        // 10-100kg续重以0.5kg计量保留1位小数；
        // 0.5为一个计算单位，举例：18.8kg按照19收费， 18.4kg按照18.5kg收费 18.1kg按照18.5kg收费
        int integer = NumberUtil.round(computeWeight, 0, RoundingMode.DOWN).intValue();
        if (NumberUtil.sub(computeWeight, integer) == 0) {
            return integer;
        }
        if (NumberUtil.sub(computeWeight, integer) <= 0.5) {
            return NumberUtil.add(integer, 0.5);
        }
        return  NumberUtil.add(integer, 1);
    }
    /**
     * 根据参数查找运费模板
     * 运费模板优先级：同城>省内>经济区互寄>跨省
     * @param waybillDTO 参数
     * @return 运费模板
     */
    private CarriageEntity findCarriage(WaybillDTO waybillDTO) {
        // 如果 发件的城市id 和 收件的城市id相同， 查询同城模板 调用findByTemplateType方法
        if (ObjectUtil.equals(waybillDTO.getReceiverCityId(), waybillDTO.getSenderCityId())) {
            // 同城
            CarriageEntity carriage = this.findByTemplateType(CarriageConstant.SAME_CITY);
            if (ObjectUtil.isNotEmpty(carriage)) {
                return carriage;
            }
        }
        // 如果没查到或不是同城，则获取收寄件地址省份id  使用AreaFeign结构查询
        // 获取收寄件地址省份id
        Long receiverProvinceId = this.areaFeign.get(waybillDTO.getReceiverCityId()).getParentId();
        Long senderProvinceId = this.areaFeign.get(waybillDTO.getSenderCityId()).getParentId();
        if (ObjectUtil.equal(receiverProvinceId, senderProvinceId)) {
            // 如果 收发件的省份id相同，查询同省的模板  调用findByTemplateType方法
            // 省内
            CarriageEntity carriage = this.findByTemplateType(CarriageConstant.SAME_PROVINCE);
            if (ObjectUtil.isNotEmpty(carriage)) {
                return carriage;
            }
        }
        // 如果没查到或不是同省，则查询是否为经济区互寄  调用findEconomicCarriage方法查询
        CarriageEntity carriage = this.findEconomicCarriage(receiverProvinceId, senderProvinceId);
        if (ObjectUtil.isNotEmpty(carriage)) {
            return carriage;
        }
        // 如果没查到或不是经济区互寄，直接查跨省运费模板
        carriage = this.findByTemplateType(CarriageConstant.TRANS_PROVINCE);
        if (ObjectUtil.isNotEmpty(carriage)) {
            return carriage;
        }
        // 如果最后没查到，直接抛自定义异常，提示模板未找到
        throw new SLException(CarriageExceptionEnum.NOT_FOUND);
    }

    /**
     * @param receiverProvinceId 收件省份id
     * @param senderProvinceId 发件省份id
     * @return
     */
    private CarriageEntity findEconomicCarriage(Long receiverProvinceId, Long senderProvinceId) {
        //通过工具类EnumUtil 获取经济区城市配置枚举
        LinkedHashMap<String, EconomicRegionEnum> EconomicRegionMap = EnumUtil.getEnumMap(EconomicRegionEnum.class);
        EconomicRegionEnum economicRegionEnum = null;
        // 遍历所有经济区枚举值
        for (EconomicRegionEnum regionEnum : EconomicRegionMap.values()) {
            //     通过ArrayUtil工具类 判断发件网点 和 收件网点是否在同一经济区
            boolean result = ArrayUtil.containsAll(regionEnum.getValue(), receiverProvinceId, senderProvinceId);
            if (result) {
                //     如果在得到对应经济区枚举
                economicRegionEnum = regionEnum;
                break;
            }
        }
        if (null == economicRegionEnum) {
            // 循环遍历未发现所属经济区，方法直接返回null
            return null;
        }
        // 如果有经济区 根据 模板类型=经济区, 运输类型=普快  关联城市=枚举的code值 查询
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.lambdaQuery(CarriageEntity.class)
                .eq(CarriageEntity::getTemplateType, CarriageConstant.ECONOMIC_ZONE)
                .eq(CarriageEntity::getTransportType, CarriageConstant.REGULAR_FAST)
                .like(CarriageEntity::getAssociatedCity, economicRegionEnum.getCode());
        return super.getOne(queryWrapper);
    }
    /**
     * 根据模板类型查询模板
     * @param templateType 模板类型：1-同城寄，2-省内寄，3-经济区互寄，4-跨省
     * @return 运费模板
     */
    private CarriageEntity findByTemplateType(Integer templateType) {
        // 根据模板类型，及运输类型 = CarriageConstant.REGULAR_FAST 查询模板
        LambdaQueryWrapper<CarriageEntity> queryWrapper = Wrappers.lambdaQuery(CarriageEntity.class)
                .eq(CarriageEntity::getTemplateType, templateType)
                .eq(CarriageEntity::getTransportType, CarriageConstant.REGULAR_FAST);
        return super.getOne(queryWrapper);
    }
}
