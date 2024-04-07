package com.sl.ms.trade.handler.wechat;

import cn.hutool.core.convert.Convert;
import cn.hutool.core.map.MapUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.sl.ms.trade.constant.TradingConstant;
import com.sl.ms.trade.entity.TradingEntity;
import com.sl.ms.trade.enums.PayChannelEnum;
import com.sl.ms.trade.enums.TradingEnum;
import com.sl.ms.trade.enums.TradingStateEnum;
import com.sl.ms.trade.handler.NativePayHandler;
import com.sl.ms.trade.handler.wechat.response.WeChatResponse;
import com.sl.transport.common.exception.SLException;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 微信扫描支付
 */
@Component("wechatNativePayHandler")
public class WechatNativePayHandler implements NativePayHandler {

    @Override
    public void createDownLineTrading(TradingEntity tradingEntity) throws SLException {
        // 查询配置
        WechatPayHttpClient client = WechatPayHttpClient.get(tradingEntity.getEnterpriseId());
        //请求地址
        String apiPath = "/v3/pay/transactions/native";

        //请求参数
        Map<String, Object> params = MapUtil.<String, Object>builder()
                .put("mchid", client.getMchId())
                .put("appid", client.getAppId())
                .put("description", tradingEntity.getMemo())
                .put("notify_url", StrUtil.replace(client.getNotifyUrl(), "{enterpriseId}", Convert.toStr(tradingEntity.getEnterpriseId())))
                .put("out_trade_no", Convert.toStr(tradingEntity.getTradingOrderNo()))
                .put("amount", MapUtil.<String, Object>builder()
                        .put("total", Convert.toInt(NumberUtil.mul(tradingEntity.getTradingAmount(), 100))) //金额，单位：分
                        .put("currency", "CNY") //人民币
                        .build())
                .build();

        //TODO day03 微信支付实现

        // 调用微信支付预下单接口

        // 如果返回结果不OK 抛出异常

        // 设置交易单中的 二维码信息 (PlaceOrderMsg存储二维码路径)

        // 设置交单单中的结果json字符串 (PlaceOrderJson存储响应结果json)

        // 设置交单中的交易状态 TradingState (设置付款中)

        // 代码捕获异常  出现异常提示支付失败

    }

    @Override
    public PayChannelEnum payChannel() {
        return PayChannelEnum.WECHAT_PAY;
    }
}
