"""
套利系统配置管理

管理套利系统的所有配置参数
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from decimal import Decimal
import yaml
from pathlib import Path

from core.logging import get_logger


@dataclass
class PrecisionConfig:
    """精度配置"""
    cache_ttl: int = 3600  # 缓存过期时间（秒）
    cleanup_interval: int = 300  # 清理间隔（秒）
    auto_refresh: bool = False  # 是否自动刷新
    refresh_interval: int = 3600  # 刷新间隔（秒）


@dataclass
class DecisionConfig:
    """决策引擎配置"""
    min_spread_threshold: float = 0.05  # 最小价差阈值（百分比）
    max_spread_threshold: float = 5.0  # 最大价差阈值（百分比）
    min_volume_threshold: float = 1000  # 最小成交量阈值
    max_position_size: float = 10000  # 最大持仓规模
    default_order_size: float = 100  # 默认订单大小
    risk_score_threshold: float = 0.8  # 风险得分阈值
    max_holding_time: int = 300  # 最大持仓时间（秒）
    opportunity_timeout: int = 30  # 机会超时时间（秒）


@dataclass
class ExecutionConfig:
    """执行引擎配置"""
    default_timeout: int = 30  # 默认超时时间（秒）
    max_retries: int = 3  # 最大重试次数
    retry_delay: float = 1.0  # 重试延迟（秒）
    order_timeout: int = 30  # 订单超时时间（秒）
    max_slippage: float = 0.01  # 最大滑点
    enable_smart_routing: bool = False  # 是否启用智能路由
    enable_twap: bool = False  # 是否启用TWAP


@dataclass
class RiskConfig:
    """风险管理配置"""
    max_daily_loss: float = 1000  # 最大日损失
    max_position_count: int = 5  # 最大持仓数量
    max_exposure_per_symbol: float = 5000  # 每个符号的最大敞口
    stop_loss_threshold: float = -100  # 止损阈值
    take_profit_threshold: float = 200  # 止盈阈值
    risk_check_interval: int = 10  # 风险检查间隔（秒）


@dataclass
class MonitoringConfig:
    """监控配置"""
    monitor_interval: int = 10  # 监控间隔（秒）
    position_timeout: int = 300  # 持仓超时时间（秒）
    enable_alerts: bool = True  # 是否启用告警
    alert_threshold: float = 0.8  # 告警阈值
    performance_tracking: bool = True  # 是否跟踪性能


@dataclass
class IntegrationConfig:
    """集成配置"""
    enable_spread_analysis: bool = True  # 是否启用价差分析集成
    enable_ticker_monitoring: bool = True  # 是否启用ticker监控集成
    enable_price_monitoring: bool = True  # 是否启用价格监控集成
    data_validation: bool = True  # 是否启用数据验证
    callback_timeout: int = 5  # 回调超时时间（秒）


@dataclass
class ArbitrageSystemConfig:
    """套利系统配置"""
    precision: PrecisionConfig = field(default_factory=PrecisionConfig)
    decision: DecisionConfig = field(default_factory=DecisionConfig)
    execution: ExecutionConfig = field(default_factory=ExecutionConfig)
    risk: RiskConfig = field(default_factory=RiskConfig)
    monitoring: MonitoringConfig = field(default_factory=MonitoringConfig)
    integration: IntegrationConfig = field(default_factory=IntegrationConfig)
    
    # 系统级配置
    enabled: bool = True
    debug_mode: bool = False
    log_level: str = "INFO"
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典格式"""
        return {
            'precision': self.precision.__dict__,
            'decision': self.decision.__dict__,
            'execution': self.execution.__dict__,
            'risk': self.risk.__dict__,
            'monitoring': self.monitoring.__dict__,
            'integration': self.integration.__dict__,
            'enabled': self.enabled,
            'debug_mode': self.debug_mode,
            'log_level': self.log_level
        }


class ArbitrageConfigManager:
    """套利配置管理器"""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径
        """
        self.config_path = config_path or "config/arbitrage/default.yaml"
        self.config = ArbitrageSystemConfig()
        self.logger = get_logger(__name__)
    
    def load_config(self, config_path: Optional[str] = None) -> ArbitrageSystemConfig:
        """
        加载配置
        
        Args:
            config_path: 配置文件路径
            
        Returns:
            套利系统配置
        """
        try:
            path = Path(config_path or self.config_path)
            
            if not path.exists():
                self.logger.warning(f"配置文件不存在: {path}，使用默认配置")
                return self.config
            
            with open(path, 'r', encoding='utf-8') as file:
                config_data = yaml.safe_load(file)
            
            # 解析配置
            self.config = self._parse_config(config_data)
            
            self.logger.info(f"配置加载成功: {path}")
            return self.config
            
        except Exception as e:
            self.logger.error(f"加载配置失败: {e}")
            return self.config
    
    def _parse_config(self, config_data: Dict[str, Any]) -> ArbitrageSystemConfig:
        """
        解析配置数据
        
        Args:
            config_data: 配置数据
            
        Returns:
            套利系统配置
        """
        try:
            # 解析各个子配置
            precision_config = PrecisionConfig(**config_data.get('precision', {}))
            decision_config = DecisionConfig(**config_data.get('decision', {}))
            execution_config = ExecutionConfig(**config_data.get('execution', {}))
            risk_config = RiskConfig(**config_data.get('risk', {}))
            monitoring_config = MonitoringConfig(**config_data.get('monitoring', {}))
            integration_config = IntegrationConfig(**config_data.get('integration', {}))
            
            # 构建系统配置
            system_config = ArbitrageSystemConfig(
                precision=precision_config,
                decision=decision_config,
                execution=execution_config,
                risk=risk_config,
                monitoring=monitoring_config,
                integration=integration_config,
                enabled=config_data.get('enabled', True),
                debug_mode=config_data.get('debug_mode', False),
                log_level=config_data.get('log_level', 'INFO')
            )
            
            return system_config
            
        except Exception as e:
            self.logger.error(f"解析配置失败: {e}")
            return ArbitrageSystemConfig()
    
    def save_config(self, config: ArbitrageSystemConfig, config_path: Optional[str] = None):
        """
        保存配置
        
        Args:
            config: 套利系统配置
            config_path: 配置文件路径
        """
        try:
            path = Path(config_path or self.config_path)
            
            # 确保目录存在
            path.parent.mkdir(parents=True, exist_ok=True)
            
            # 转换为字典格式
            config_dict = config.to_dict()
            
            # 保存配置
            with open(path, 'w', encoding='utf-8') as file:
                yaml.dump(config_dict, file, default_flow_style=False, 
                         allow_unicode=True, sort_keys=False)
            
            self.logger.info(f"配置保存成功: {path}")
            
        except Exception as e:
            self.logger.error(f"保存配置失败: {e}")
    
    def get_config(self) -> ArbitrageSystemConfig:
        """获取当前配置"""
        return self.config
    
    def update_config(self, updates: Dict[str, Any]):
        """
        更新配置
        
        Args:
            updates: 更新的配置项
        """
        try:
            # 更新配置
            for key, value in updates.items():
                if hasattr(self.config, key):
                    if isinstance(getattr(self.config, key), (PrecisionConfig, DecisionConfig, 
                                                             ExecutionConfig, RiskConfig, 
                                                             MonitoringConfig, IntegrationConfig)):
                        # 更新子配置
                        sub_config = getattr(self.config, key)
                        for sub_key, sub_value in value.items():
                            if hasattr(sub_config, sub_key):
                                setattr(sub_config, sub_key, sub_value)
                    else:
                        setattr(self.config, key, value)
            
            self.logger.info("配置更新成功")
            
        except Exception as e:
            self.logger.error(f"更新配置失败: {e}")
    
    def validate_config(self, config: ArbitrageSystemConfig) -> bool:
        """
        验证配置
        
        Args:
            config: 套利系统配置
            
        Returns:
            是否有效
        """
        try:
            # 验证决策配置
            if config.decision.min_spread_threshold >= config.decision.max_spread_threshold:
                self.logger.error("最小价差阈值不能大于等于最大价差阈值")
                return False
            
            if config.decision.min_volume_threshold <= 0:
                self.logger.error("最小成交量阈值必须大于0")
                return False
            
            # 验证执行配置
            if config.execution.default_timeout <= 0:
                self.logger.error("默认超时时间必须大于0")
                return False
            
            if config.execution.max_retries < 0:
                self.logger.error("最大重试次数不能小于0")
                return False
            
            # 验证风险配置
            if config.risk.max_daily_loss <= 0:
                self.logger.error("最大日损失必须大于0")
                return False
            
            if config.risk.max_position_count <= 0:
                self.logger.error("最大持仓数量必须大于0")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证配置失败: {e}")
            return False
    
    def create_default_config_file(self, config_path: Optional[str] = None):
        """
        创建默认配置文件
        
        Args:
            config_path: 配置文件路径
        """
        try:
            path = Path(config_path or self.config_path)
            
            # 创建默认配置
            default_config = ArbitrageSystemConfig()
            
            # 保存默认配置
            self.save_config(default_config, str(path))
            
            self.logger.info(f"创建默认配置文件: {path}")
            
        except Exception as e:
            self.logger.error(f"创建默认配置文件失败: {e}")
    
    # TODO: 高级功能占位符
    def enable_hot_reload(self, callback: Optional[callable] = None):
        """
        启用热重载
        
        Args:
            callback: 配置变更回调函数
        """
        # TODO: 实现配置热重载
        pass
    
    def get_environment_overrides(self) -> Dict[str, Any]:
        """
        获取环境变量覆盖
        
        Returns:
            环境变量覆盖配置
        """
        # TODO: 实现环境变量覆盖
        return {}
    
    def merge_configs(self, base_config: ArbitrageSystemConfig, 
                     override_config: Dict[str, Any]) -> ArbitrageSystemConfig:
        """
        合并配置
        
        Args:
            base_config: 基础配置
            override_config: 覆盖配置
            
        Returns:
            合并后的配置
        """
        # TODO: 实现配置合并
        return base_config 