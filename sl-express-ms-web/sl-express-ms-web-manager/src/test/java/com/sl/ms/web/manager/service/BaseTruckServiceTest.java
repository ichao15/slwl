package com.sl.ms.web.manager.service;

import com.sl.ms.web.manager.vo.baseTruck.TruckVO;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.util.List;

@Slf4j
@SpringBootTest
class BaseTruckServiceTest {

    @Resource
    private BaseTruckService baseTruckService;

    @Test
    void workingTrucks() {
        List<TruckVO> truckVOS = baseTruckService.workingTrucks();
        log.info(truckVOS + "");
    }
}