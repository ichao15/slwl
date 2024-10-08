package com.sl.ms.trade.domain.request;

import com.sl.ms.trade.enums.PayChannelEnum;
import io.swagger.annotations.ApiModelProperty;
import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.math.BigDecimal;

/**
 * @author zzj
 * @version 1.0
 */
@Data
public class NativePayDTO {

    @ApiModelProperty(value = "商户号", required = true)
    private Long enterpriseId;
    @ApiModelProperty(value = "业务系统订单号", required = true)
    private Long productOrderNo;
    @ApiModelProperty(value = "支付渠道", required = true)
    private PayChannelEnum tradingChannel;
    @ApiModelProperty(value = "交易金额，单位：元", required = true)
    private BigDecimal tradingAmount;
    @ApiModelProperty(value = "备注，如：运费", required = true)
    private String memo;
}
