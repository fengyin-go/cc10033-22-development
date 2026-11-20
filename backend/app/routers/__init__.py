"""业务路由汇总：专用领域路由先注册，通用资源路由兜底匹配 /api/{module}。"""
from __future__ import annotations

from app.routers import partition_baseline, resource

# 分区恢复基线是固定前缀路由，必须排在通配 /api/{module} 资源路由之前，
# 否则会被动态路径抢先匹配成模块名。
ROUTERS = [partition_baseline, resource]
