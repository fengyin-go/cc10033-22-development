<template>
  <div class="app-shell">
    <aside class="app-side">
      <h1 class="app-title">维度建模开发中心</h1>
      <nav class="nav-list">
        <RouterLink v-for="item in navItems" :key="item.path" :to="item.path" class="nav-item">
          {{ item.label }}
        </RouterLink>
      </nav>
    </aside>
    <main class="app-main">
      <header class="app-head">
        <span class="head-desc">面向数据仓库建设场景，提供维度建模、事实表设计、缓慢变化维和汇总层构建的建模平台</span>
        <span class="head-user">当前值班：数据工程组</span>
      </header>
      <RouterView />
    </main>
  </div>
</template>

<script setup lang="ts">
const navItems = [{ label: "运营概览", path: "/" }, { label: "维度定义", path: "/dim_definition" }, { label: "维度属性", path: "/dim_attr" }, { label: "事实设计", path: "/fact_design" }, { label: "度量配置", path: "/measure_config" }, { label: "SCD处理", path: "/scd_handler" }, { label: "代理键", path: "/surrogate_key" }, { label: "层级构建", path: "/hierarchy_build" }, { label: "星型查询", path: "/star_query" }, { label: "汇总构建", path: "/agg_summary" }, { label: "桥接关联", path: "/dim_bridge" }, { label: "ETL映射", path: "/etl_mapping" }, { label: "快照计划", path: "/snapshot_plan" }, { label: "分区恢复基线", path: "/partition_baseline" }, { label: "模型血缘", path: "/model_lineage" }, { label: "一致性维度", path: "/conform_dim" }, { label: "DataVault", path: "/data_vault" }, { label: "多维立方", path: "/cube_builder" }, { label: "变更日志", path: "/slow_change_log" }, { label: "日期维度", path: "/date_dim_gen" }, { label: "模型校验", path: "/model_validate" }, { label: "部署流水线", path: "/deploy_pipeline" }]
</script>
