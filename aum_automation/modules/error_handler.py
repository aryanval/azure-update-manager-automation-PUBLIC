"""
Error Handler - Safety-First Error Management
Handles all errors with conservative approach: when in doubt, skip and flag
"""

import logging
from typing import Tuple, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorAction(Enum):
    """Actions to take when encountering an error"""
    RETRY = "retry"
    SKIP = "skip"
    ABORT = "abort"
    MANUAL = "manual_review_required"


class ErrorCategory(Enum):
    """Categories of errors we might encounter"""
    CONNECTION = "connection"
    PROVISIONING = "provisioning"
    PERMISSION = "permission"
    ENVIRONMENT = "environment"
    VM_BUSY = "vm_busy"
    NOT_FOUND = "not_found"
    UNKNOWN = "unknown"


class ErrorHandler:
    """
    Central error handling with safety-first approach.
    
    GOLDEN RULE: When in doubt, SKIP and FLAG for manual review.
    NEVER take action that requires client permission without confirmation.
    """
    
    # Error patterns mapped to categories and actions
    ERROR_PATTERNS = {
        # Retriable errors - can retry with backoff
        'unable to connect': (ErrorCategory.CONNECTION, ErrorAction.RETRY),
        'connection timeout': (ErrorCategory.CONNECTION, ErrorAction.RETRY),
        'vm busy': (ErrorCategory.VM_BUSY, ErrorAction.RETRY),
        'operation in progress': (ErrorCategory.VM_BUSY, ErrorAction.RETRY),
        'operation already in progress': (ErrorCategory.VM_BUSY, ErrorAction.RETRY),
        
        # Skip errors - don't retry, just skip and log
        'os provisioning error': (ErrorCategory.PROVISIONING, ErrorAction.SKIP),
        'provisioning failed': (ErrorCategory.PROVISIONING, ErrorAction.SKIP),
        'customer environment error': (ErrorCategory.ENVIRONMENT, ErrorAction.SKIP),
        'environment error': (ErrorCategory.ENVIRONMENT, ErrorAction.SKIP),
        'access denied': (ErrorCategory.PERMISSION, ErrorAction.SKIP),
        'insufficient permissions': (ErrorCategory.PERMISSION, ErrorAction.SKIP),
        'authorization failed': (ErrorCategory.PERMISSION, ErrorAction.SKIP),
        'not found': (ErrorCategory.NOT_FOUND, ErrorAction.SKIP),
        'resource not found': (ErrorCategory.NOT_FOUND, ErrorAction.SKIP),
        'does not exist': (ErrorCategory.NOT_FOUND, ErrorAction.SKIP),
    }
    
    def __init__(self, config):
        self.config = config
        self.retry_config = config.get_retry_config()
        self.safety_config = config.get_safety_config()
    
    def categorize_error(self, error: Exception) -> Tuple[ErrorCategory, ErrorAction]:
        """
        Categorize an error and determine appropriate action.
        
        Returns:
            (category, action) tuple
        """
        error_msg = str(error).lower()
        
        # Check against known patterns
        for pattern, (category, action) in self.ERROR_PATTERNS.items():
            if pattern in error_msg:
                logger.info(f"Error categorized as {category.value}: {action.value}")
                return category, action
        
        # Unknown error - default to SKIP if configured (safety first!)
        if self.safety_config.get('skip_on_unknown_error', True):
            logger.warning(f"Unknown error encountered, defaulting to SKIP: {error}")
            return ErrorCategory.UNKNOWN, ErrorAction.MANUAL
        else:
            logger.error(f"Unknown error encountered: {error}")
            return ErrorCategory.UNKNOWN, ErrorAction.ABORT
    
    def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determine if an operation should be retried.
        
        Args:
            error: The exception that occurred
            attempt: Current attempt number (1-indexed)
        
        Returns:
            True if should retry, False otherwise
        """
        category, action = self.categorize_error(error)
        
        if action != ErrorAction.RETRY:
            return False
        
        max_attempts = self.retry_config.get('max_attempts', 2)
        
        if attempt >= max_attempts:
            logger.warning(f"Max retry attempts ({max_attempts}) reached for: {error}")
            return False
        
        logger.info(f"Will retry (attempt {attempt}/{max_attempts}): {error}")
        return True
    
    def get_backoff_time(self, attempt: int) -> int:
        """Get backoff time in seconds for retry attempt"""
        base_backoff = self.retry_config.get('backoff_seconds', 30)
        # Simple linear backoff: 30s, 60s, 90s...
        return base_backoff * attempt
    
    def handle_error(
        self, 
        error: Exception, 
        vm_name: str, 
        operation: str,
        attempt: int = 1
    ) -> dict:
        """
        Handle an error and return action details.
        
        Args:
            error: The exception
            vm_name: Name of the VM
            operation: What operation was being performed
            attempt: Current attempt number
        
        Returns:
            dict with keys: action, category, message, should_retry, backoff_seconds
        """
        category, action = self.categorize_error(error)
        
        result = {
            'vm_name': vm_name,
            'operation': operation,
            'error': str(error),
            'category': category.value,
            'action': action.value,
            'should_retry': False,
            'backoff_seconds': 0,
            'message': ''
        }
        
        # Determine specific response
        if action == ErrorAction.RETRY and self.should_retry(error, attempt):
            result['should_retry'] = True
            result['backoff_seconds'] = self.get_backoff_time(attempt)
            result['message'] = (
                f"Retriable error on {vm_name} during {operation}. "
                f"Will retry after {result['backoff_seconds']}s (attempt {attempt})"
            )
            logger.info(result['message'])
        
        elif action == ErrorAction.SKIP:
            result['message'] = (
                f"Skipping {vm_name} due to {category.value} error during {operation}: {error}"
            )
            logger.warning(result['message'])
        
        elif action == ErrorAction.MANUAL:
            result['message'] = (
                f"MANUAL REVIEW REQUIRED for {vm_name}. "
                f"Unknown error during {operation}: {error}"
            )
            logger.error(result['message'])
        
        elif action == ErrorAction.ABORT:
            result['message'] = (
                f"ABORTING operation for {vm_name} due to critical error: {error}"
            )
            logger.error(result['message'])
        
        return result
    
    def format_error_for_excel(self, error_info: dict) -> str:
        """
        Format error information for Excel sheet.
        
        Args:
            error_info: Result from handle_error()
        
        Returns:
            Formatted error string for Excel
        """
        action = error_info['action']
        category = error_info['category']
        error_msg = error_info['error']
        
        if action == 'skip':
            return f"SKIPPED - {category}: {error_msg}"
        elif action == 'manual_review_required':
            return f"MANUAL REVIEW - Unknown error: {error_msg}"
        elif action == 'abort':
            return f"ABORTED - Critical error: {error_msg}"
        else:
            return f"{category}: {error_msg}"
    
    def is_vm_skippable_by_tags(self, vm_tags: dict) -> Tuple[bool, Optional[str]]:
        """
        Check if VM should be skipped based on tags.
        
        Args:
            vm_tags: Dictionary of VM tags
        
        Returns:
            (should_skip, reason) tuple
        """
        if not vm_tags:
            return False, None
        
        skip_tag_patterns = self.config.get_skip_conditions().get('tags', [])
        
        for pattern in skip_tag_patterns:
            if '=' in pattern:
                key, value = pattern.split('=', 1)
                if vm_tags.get(key) == value:
                    return True, f"Tag: {key}={value}"
            else:
                # Just check if tag key exists
                if pattern in vm_tags:
                    return True, f"Tag: {pattern} exists"
        
        return False, None
    
    def log_error_summary(self, errors: list):
        """Log a summary of all errors encountered"""
        if not errors:
            logger.info("No errors encountered - all operations completed successfully")
            return
        
        logger.warning(f"\n{'='*60}")
        logger.warning(f"ERROR SUMMARY - {len(errors)} error(s) encountered")
        logger.warning(f"{'='*60}")
        
        # Group by action
        by_action = {}
        for err in errors:
            action = err.get('action', 'unknown')
            if action not in by_action:
                by_action[action] = []
            by_action[action].append(err)
        
        for action, err_list in by_action.items():
            logger.warning(f"\n{action.upper()}: {len(err_list)} VM(s)")
            for err in err_list:
                logger.warning(f"  - {err['vm_name']}: {err['error']}")
        
        logger.warning(f"{'='*60}\n")
