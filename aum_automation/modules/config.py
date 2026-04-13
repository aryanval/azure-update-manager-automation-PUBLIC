"""
Configuration Manager
Loads and validates configuration from config.yaml
"""

import os
import yaml
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class Config:
    """Configuration manager for AUM automation"""
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            config_path = os.path.join(
                os.path.dirname(__file__), 
                '..', 
                'config', 
                'config.yaml'
            )
        
        self.config_path = config_path
        self.config = self._load_config()
        self._validate_config()
    
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
            logger.info(f"Configuration loaded from {self.config_path}")
            return config
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_path}")
            raise
        except yaml.YAMLError as e:
            logger.error(f"Error parsing configuration file: {e}")
            raise
    
    def _validate_config(self):
        """Validate configuration has required fields"""
        required_sections = ['tenants', 'patch_exclusions', 'excel', 'retry', 'logging', 'safety']
        
        for section in required_sections:
            if section not in self.config:
                raise ValueError(f"Missing required configuration section: {section}")
        
        # Validate tenants
        if not self.config['tenants']:
            raise ValueError("No tenants configured")
        
        logger.info("Configuration validation passed")
    
    def get_tenant_config(self, tenant_name: str) -> Dict:
        """Get configuration for a specific tenant"""
        if tenant_name not in self.config['tenants']:
            raise ValueError(f"Tenant '{tenant_name}' not found in configuration")
        
        return self.config['tenants'][tenant_name]
    
    def get_subscription_id(self, tenant_name: str) -> str:
        """Get primary subscription ID for a tenant"""
        sub_ids = self.get_subscription_ids(tenant_name)
        if not sub_ids:
            raise ValueError(f"Subscription ID not configured for tenant: {tenant_name}")
        return sub_ids[0]

    def get_subscription_ids(self, tenant_name: str) -> List[str]:
        """Get one or more subscription IDs for a tenant"""
        tenant_config = self.get_tenant_config(tenant_name)

        # Backward compatible: single subscription_id
        sub_id = tenant_config.get('subscription_id', '')
        if sub_id:
            return [sub_id]

        # New format: multiple subscription_ids
        sub_ids = tenant_config.get('subscription_ids', [])
        if isinstance(sub_ids, list):
            return [s for s in sub_ids if s]

        return []
    
    def get_resource_groups(self, tenant_name: str) -> List[str]:
        """Get resource groups for a tenant (empty list means all)"""
        tenant_config = self.get_tenant_config(tenant_name)
        return tenant_config.get('resource_groups', [])
    
    def get_always_exclude_patches(self) -> List[str]:
        """Get patches that should always be excluded"""
        return self.config['patch_exclusions'].get('always_exclude', [])
    
    def get_skip_conditions(self) -> Dict:
        """Get VM skip conditions"""
        return self.config.get('skip_conditions', {})
    
    def get_excel_config(self) -> Dict:
        """Get Excel configuration"""
        return self.config['excel']
    
    def get_retry_config(self) -> Dict:
        """Get retry configuration"""
        return self.config['retry']
    
    def get_polling_config(self) -> Dict:
        """Get polling configuration"""
        return self.config.get('polling', {})
    
    def get_logging_config(self) -> Dict:
        """Get logging configuration"""
        return self.config['logging']
    
    def get_safety_config(self) -> Dict:
        """Get safety configuration"""
        return self.config['safety']
    
    def is_dry_run(self) -> bool:
        """Check if dry-run mode is enabled"""
        return self.config['safety'].get('dry_run_mode', False)
    
    def skip_on_unknown_error(self) -> bool:
        """Check if we should skip on unknown errors"""
        return self.config['safety'].get('skip_on_unknown_error', True)
    
    def get_tenant_list(self) -> List[str]:
        """Get list of configured tenant names"""
        return list(self.config['tenants'].keys())
    
    def is_retriable_error(self, error_msg: str) -> bool:
        """Check if an error message indicates a retriable error"""
        error_msg_lower = error_msg.lower()
        retriable = self.config['retry'].get('retriable_errors', [])
        
        return any(err.lower() in error_msg_lower for err in retriable)
    
    def should_skip_error(self, error_msg: str) -> bool:
        """Check if an error message indicates we should skip the VM"""
        error_msg_lower = error_msg.lower()
        skip_errors = self.config['retry'].get('skip_errors', [])
        
        return any(err.lower() in error_msg_lower for err in skip_errors)


# Singleton instance
_config_instance: Optional[Config] = None


def get_config(config_path: str = None) -> Config:
    """Get configuration singleton instance"""
    global _config_instance
    
    if _config_instance is None:
        _config_instance = Config(config_path)
    
    return _config_instance
