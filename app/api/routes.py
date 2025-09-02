"""
API路由定义
提供历史数据查询接口
"""

from fastapi import APIRouter, HTTPException, Query, Depends
from typing import List, Optional
from datetime import datetime, timedelta

from ..models.data_models import (
    QueryRequest, QueryResponse, StatisticsRequest, StatisticsResponse,
    HistoryData, HealthStatus, DataMetrics
)
from ..services.query_service import query_service
from ..services.data_collector import data_collector
from ..services.data_storage import data_storage
from ..services.scheduler import scheduler_service
from ..core.config import settings

# 创建路由器
router = APIRouter(prefix=settings.API_PREFIX)

@router.get("/health", response_model=HealthStatus, summary="健康检查")
async def health_check():
    """系统健康检查"""
    try:
        # 检查各组件状态
        redis_ok = data_collector.test_connection()
        influxdb_ok = data_storage.test_connection()
        scheduler_status = scheduler_service.get_status()
        
        components = {
            "redis": {
                "status": "healthy" if redis_ok else "unhealthy",
                "message": "Redis连接正常" if redis_ok else "Redis连接异常"
            },
            "influxdb": {
                "status": "healthy" if influxdb_ok else "unhealthy", 
                "message": "InfluxDB连接正常" if influxdb_ok else "InfluxDB连接异常"
            },
            "scheduler": {
                "status": "healthy" if scheduler_status["is_running"] else "unhealthy",
                "message": f"调度器运行状态: {'运行中' if scheduler_status['is_running'] else '已停止'}",
                "job_count": scheduler_status["job_count"]
            }
        }
        
        # 判断整体状态
        overall_status = "healthy" if all(
            comp["status"] == "healthy" for comp in components.values()
        ) else "unhealthy"
        
        return HealthStatus(
            status=overall_status,
            timestamp=datetime.utcnow(),
            components=components
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"健康检查失败: {e}")


@router.get("/data/query", response_model=QueryResponse, summary="查询历史数据")
async def query_history_data(
    start_time: datetime = Query(..., description="开始时间"),
    end_time: datetime = Query(..., description="结束时间"),
    redis_keys: Optional[str] = Query(None, description="Redis键列表，逗号分隔"),
    point_ids: Optional[str] = Query(None, description="点位ID列表，逗号分隔"),
    sources: Optional[str] = Query(None, description="数据来源列表，逗号分隔"),
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(100, ge=1, le=1000, description="每页大小")
):
    """查询历史数据"""
    try:
        # 验证时间范围
        if end_time <= start_time:
            raise HTTPException(status_code=400, detail="结束时间必须大于开始时间")
        
        time_diff = end_time - start_time
        if time_diff > timedelta(days=settings.MAX_TIME_RANGE_DAYS):
            raise HTTPException(
                status_code=400, 
                detail=f"查询时间范围不能超过 {settings.MAX_TIME_RANGE_DAYS} 天"
            )
        
        # 解析参数
        parsed_redis_keys = None
        if redis_keys:
            parsed_redis_keys = [key.strip() for key in redis_keys.split(',')]
        
        parsed_point_ids = None
        if point_ids:
            parsed_point_ids = [pid.strip() for pid in point_ids.split(',')]
        
        parsed_sources = None
        if sources:
            parsed_sources = [src.strip() for src in sources.split(',')]
        
        # 创建查询请求
        request = QueryRequest(
            start_time=start_time,
            end_time=end_time,
            redis_keys=parsed_redis_keys,
            point_ids=parsed_point_ids,
            sources=parsed_sources,
            page=page,
            page_size=page_size
        )
        
        # 执行查询
        response = query_service.query_history_data(request)
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询失败: {e}")

@router.get("/data/statistics", response_model=StatisticsResponse, summary="查询统计数据")
async def query_statistics(
    start_time: datetime = Query(..., description="开始时间"),
    end_time: datetime = Query(..., description="结束时间"),
    redis_key: str = Query(..., description="Redis键"),
    point_id: str = Query(..., description="点位ID"),
    aggregation: str = Query("mean", description="聚合方式"),
    interval: str = Query("1h", description="时间间隔")
):
    """查询统计数据"""
    try:
        # 验证聚合方式
        valid_aggregations = ["mean", "sum", "min", "max", "count"]
        if aggregation not in valid_aggregations:
            raise HTTPException(
                status_code=400, 
                detail=f"无效的聚合方式，支持: {', '.join(valid_aggregations)}"
            )
        
        # 验证时间间隔
        valid_intervals = ["1m", "5m", "15m", "30m", "1h", "2h", "6h", "12h", "1d"]
        if interval not in valid_intervals:
            raise HTTPException(
                status_code=400,
                detail=f"无效的时间间隔，支持: {', '.join(valid_intervals)}"
            )
        
        # 创建统计请求
        request = StatisticsRequest(
            start_time=start_time,
            end_time=end_time,
            redis_key=redis_key,
            point_id=point_id,
            aggregation=aggregation,
            interval=interval
        )
        
        # 执行查询
        response = query_service.query_statistics(request)
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"查询统计数据失败: {e}")

@router.get("/data/latest", response_model=HistoryData, summary="获取最新数据")
async def get_latest_data(
    redis_key: str = Query(..., description="Redis键"),
    point_id: str = Query(..., description="点位ID")
):
    """获取指定点位的最新数据"""
    try:
        data = query_service.get_latest_data(redis_key, point_id)
        
        if not data:
            raise HTTPException(status_code=404, detail="未找到数据")
        
        return data
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取最新数据失败: {e}")

@router.get("/data/range", summary="获取数据范围信息")
async def get_data_range():
    """获取数据范围信息"""
    try:
        range_info = query_service.get_data_range_info()
        return range_info
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据范围信息失败: {e}")

@router.get("/channels", summary="获取通道列表")
async def get_channels():
    """获取所有通道列表"""
    try:
        channels = data_collector.get_channel_list()
        return {"channels": channels, "count": len(channels)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取通道列表失败: {e}")

@router.get("/metrics", response_model=DataMetrics, summary="获取数据指标")
async def get_data_metrics():
    """获取数据指标"""
    try:
        # 获取存储统计
        storage_stats = data_storage.get_storage_stats()
        
        # 获取通道信息
        channels = data_collector.get_channel_list()
        
        # 获取数据范围信息
        range_info = query_service.get_data_range_info()
        
        return DataMetrics(
            total_points=storage_stats.get("total_points", 0),
            points_today=0,  # 简化实现
            points_last_hour=0,  # 简化实现
            storage_size=storage_stats.get("storage_size", 0),
            latest_timestamp=range_info.get("latest_timestamp"),
            channels_count=len(channels),
            active_channels=len([c for c in channels if c])  # 简化实现
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取数据指标失败: {e}")

@router.get("/scheduler/status", summary="获取调度器状态")
async def get_scheduler_status():
    """获取调度器状态"""
    try:
        status = scheduler_service.get_status()
        return status
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取调度器状态失败: {e}")


