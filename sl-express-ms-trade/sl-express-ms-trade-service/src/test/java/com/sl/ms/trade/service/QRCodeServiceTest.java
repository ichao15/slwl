package com.sl.ms.trade.service;

import com.sl.ms.trade.enums.PayChannelEnum;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author zzj
 * @version 1.0
 */
@SpringBootTest
class QRCodeServiceTest {

    @Resource
    private QRCodeService qrCodeService;

    @Test
    void generate() {
        String qr = this.qrCodeService.generate("神领物流");
        System.out.println(qr);
    }

    @Test
    void testGenerate() {
        String qr = this.qrCodeService.generate("微信支付", PayChannelEnum.WECHAT_PAY);
        System.out.println(qr);
        String qr2 = this.qrCodeService.generate("支付宝支付", PayChannelEnum.ALI_PAY);
        System.out.println(qr2);
    }
}