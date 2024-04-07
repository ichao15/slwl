package com.sl.ms.carriage.service;

import com.sl.ms.carriage.domain.dto.CarriageDTO;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class CarriageServiceTest {

    @Resource
    private CarriageService carriageService;

    @Test
    void saveOrUpdate() {
    }

    @Test
    void findAll() {
        List<CarriageDTO> list = this.carriageService.findAll();
        list.forEach(System.out::println);
    }

    @Test
    void compute() {
    }
}