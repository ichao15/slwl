package com.sl.transport.info.mq;
import cn.hutool.core.convert.Convert;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.transport.api.OrganFeign;
import com.sl.transport.common.constant.Constants;
import com.sl.transport.common.vo.TransportInfoMsg;
import com.sl.transport.domain.OrganDTO;
import com.sl.transport.info.entity.TransportInfoDetail;
import com.sl.transport.info.service.TransportInfoService;
import org.springframework.amqp.core.ExchangeTypes;
import org.springframework.amqp.rabbit.annotation.Exchange;
import org.springframework.amqp.rabbit.annotation.Queue;
import org.springframework.amqp.rabbit.annotation.QueueBinding;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;
import javax.annotation.Resource;
/**
 * 物流信息消息
 */
@Component
public class TransportInfoMQListener {
    @Resource
    private OrganFeign organFeign;
    @Resource
    private TransportInfoService transportInfoService;
    @RabbitListener(bindings = @QueueBinding(
            value = @Queue(name = Constants.MQ.Queues.TRANSPORT_INFO_APPEND),
            exchange = @Exchange(name = Constants.MQ.Exchanges.TRANSPORT_INFO, type = ExchangeTypes.TOPIC),
            key = Constants.MQ.RoutingKeys.TRANSPORT_INFO_APPEND
    ))
    public void listenTransportInfoMsg(String msg) {
        //{"info":"您的快件已到达【$organId】", "status":"运输中", "organId":90001, "transportOrderId":920733749248 , "created":1653133234913}
        // 注意：  如果消息内容中有 $organId 需要我们将其替换为机构名称
        TransportInfoMsg transportInfoMsg = JSONUtil.toBean(msg, TransportInfoMsg.class);
        // TODO day10 完成物流信息消息消费

        // 1. 消息中得到 机构id  运单id 和 info信息
        Long organId = transportInfoMsg.getOrganId();
        String transportOrderId = Convert.toStr(transportInfoMsg.getTransportOrderId());
        String info = transportInfoMsg.getInfo();
        // 2. 判断 info消息中是否包含 $organId 字符串
        if (StrUtil.contains(info, "$organId")) {
            // 2.1 如果包含 organFeign远程查询机构信息
            OrganDTO organDTO = this.organFeign.queryById(organId);
            if (organDTO == null) {
                return;
            }
            // 2.2 使用得到的机构名称 替换info字符串
            info = StrUtil.replace(info, "$organId", organDTO.getName());
        }
        // 3. 封装 TransportInfoDetail信息对象
        TransportInfoDetail infoDetail = TransportInfoDetail.builder()
                .info(info)
                .status(transportInfoMsg.getStatus())
                .created(transportInfoMsg.getCreated()).build();
        // 4. 保存到MongoDB中 transportInfoService
        this.transportInfoService.saveOrUpdate(transportOrderId, infoDetail);
    }
}