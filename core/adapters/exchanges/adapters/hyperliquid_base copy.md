    def _setup_symbol_mappings(self):
        """设置符号映射"""
        # 🚀 根据配置文件创建符号映射
        self._default_symbol_mapping = {}
        
        # 🔥 优先使用配置文件中的映射
        if hasattr(self, 'market_config') and self.market_config:
            symbol_mappings = self.market_config.get('symbol_mapping', {})
            
            # 添加 Backpack 格式映射（最重要的映射）
            backpack_mappings = symbol_mappings.get('backpack_to_hyperliquid', {})
            if backpack_mappings:
                self._default_symbol_mapping.update(backpack_mappings)
                if self.logger:
                    self.logger.info(f"✅ 从配置文件加载 {len(backpack_mappings)} 个Backpack格式映射")
            
            # 添加永续合约映射
            if hasattr(self, 'perpetual_enabled') and self.perpetual_enabled:
                perpetual_mappings = symbol_mappings.get('perpetual', {})
                if perpetual_mappings:
                    self._default_symbol_mapping.update(perpetual_mappings)
                    if self.logger:
                        self.logger.info(f"✅ 从配置文件加载 {len(perpetual_mappings)} 个永续合约映射")
            
            # 添加现货映射
            if hasattr(self, 'spot_enabled') and self.spot_enabled:
                spot_mappings = symbol_mappings.get('spot', {})
                if spot_mappings:
                    self._default_symbol_mapping.update(spot_mappings)
                    if self.logger:
                        self.logger.info(f"✅ 从配置文件加载 {len(spot_mappings)} 个现货映射")
        
        # 🔥 备用映射：如果配置文件映射失败，使用基本映射
        if not self._default_symbol_mapping:
            if self.logger:
                self.logger.warning("⚠️  配置文件中没有符号映射，使用备用硬编码映射")
            
            # 基本的Backpack格式映射
            self._default_symbol_mapping = {
                # 🔥 核心映射：Backpack格式 -> Hyperliquid格式
                "BTC_USDC_PERP": "BTC/USDC:USDC",
                "ETH_USDC_PERP": "ETH/USDC:USDC",
                "SOL_USDC_PERP": "SOL/USDC:USDC",
                "AVAX_USDC_PERP": "AVAX/USDC:USDC",
                "DOGE_USDC_PERP": "DOGE/USDC:USDC",
                "ADA_USDC_PERP": "ADA/USDC:USDC",
                "LINK_USDC_PERP": "LINK/USDC:USDC",
                
                # 标准格式保持不变
                "BTC/USDC:USDC": "BTC/USDC:USDC",
                "ETH/USDC:USDC": "ETH/USDC:USDC", 
                "SOL/USDC:USDC": "SOL/USDC:USDC",
                "AVAX/USDC:USDC": "AVAX/USDC:USDC"
            }

        # 合并用户配置的符号映射（最高优先级）
        if self.config and hasattr(self.config, 'symbol_mapping') and self.config.symbol_mapping:
            self._default_symbol_mapping.update(self.config.symbol_mapping)
            if self.logger:
                self.logger.info(f"✅ 从用户配置加载 {len(self.config.symbol_mapping)} 个额外映射")

    def map_symbol(self, symbol: str) -> str:
        """映射交易对符号到Hyperliquid格式"""
        # 🔍 调试：检查映射字典内容
        if self.logger and not hasattr(self, '_debug_logged'):
            self.logger.info(f"🔍 映射字典大小: {len(self._default_symbol_mapping)}")
            if self._default_symbol_mapping:
                sample_mappings = dict(list(self._default_symbol_mapping.items())[:3])
                self.logger.info(f"🔍 映射字典示例: {sample_mappings}")
            self._debug_logged = True
        
        mapped = self._default_symbol_mapping.get(symbol, symbol)
        
        # 添加调试日志
        if self.logger and mapped != symbol:
            self.logger.info(f"🔄 Hyperliquid符号映射: {symbol} -> {mapped}")
        elif self.logger and mapped == symbol:
            self.logger.warning(f"⚠️  Hyperliquid符号未映射: {symbol} (保持原样)")
            
        return mapped

    def reverse_map_symbol(self, exchange_symbol: str) -> str:
        """反向映射交易对符号从Hyperliquid格式"""
        # 🔧 修复：首先尝试完整映射
        reverse_mapping = {v: k for k, v in self._default_symbol_mapping.items()}
        result = reverse_mapping.get(exchange_symbol, None)
        
        if result:
            return result
        
        # 🚀 修复：根据配置的市场类型进行转换
        if exchange_symbol and '/' not in exchange_symbol and ':' not in exchange_symbol:
            # 这是一个基础币种，转换为标准格式
            base_coin = exchange_symbol.upper()
            
            # 🔥 修复：统一使用:USDC格式以匹配系统其他部分
            standard_symbol = f"{base_coin}/USDC:USDC"
            
            # 🔧 修复：添加调试信息
            if self.logger:
                self.logger.debug(f"🔄 动态币种转换: {exchange_symbol} -> {standard_symbol} (统一格式)")
            
            return standard_symbol
        
        # 🔧 修复：对于其他格式，尝试规范化后再转换
        if exchange_symbol:
            # 尝试处理不同的格式
            if exchange_symbol.endswith('-USD'):
                # BTC-USD -> BTC/USDC:USDC（统一格式）
                base_coin = exchange_symbol.replace('-USD', '')
                standard_symbol = f"{base_coin}/USDC:USDC"
                if self.logger:
                    self.logger.debug(f"🔄 USD格式转换: {exchange_symbol} -> {standard_symbol}")
                return standard_symbol
            elif exchange_symbol.endswith('/USDC:USDC'):
                # BTC/USDC:USDC -> BTC/USDC:USDC（保持不变）
                if self.logger:
                    self.logger.debug(f"🔄 USDC格式保持: {exchange_symbol}")
                return exchange_symbol
        
        # 如果都没有匹配，返回原值
        if self.logger:
            self.logger.debug(f"⚠️  无法转换符号: {exchange_symbol}")
        return exchange_symbol

->

    # symbol mapping相关方法已移除 - 现在由执行器统一处理格式转换