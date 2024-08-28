package com.hmdp.dto;

import lombok.Data;

import java.util.List;

@Data
public class ScrollResult {
    // 分页结果列表
    private List<?> list;
    // 下次查询的最大时间戳
    private Long minTime;
    // 与上次查询最小值一样的个数
    private Integer offset;
}
