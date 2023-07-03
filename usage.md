feat1-函数sql
做 POS 核算这张动态表吧; 做一版 dbt_sr 逻辑; 需求指标, 模型设计, 开发, 与dw原有维表核对
- 1h 先抽出一部分
- 


[] 创建项目, 跑通 dbt run 
[] 尝试迁移一个模型, 并配置增量, 调度

知识库: https://www.notion.so/starRocks-DBT-b5bd528412854a19bcc343a3e8070984


巨坑:
1. ['debug效率低',] 多个cte语句时, 报错信息位置会有问题; eg: 第二个CTE有问题, 但是报错显示在第一个CTE中
