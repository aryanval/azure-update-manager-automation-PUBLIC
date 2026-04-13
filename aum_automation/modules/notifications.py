"""
Notification System
Alerts user for patch failures, manual interventions, and completion summaries
"""

import logging
from typing import List, Dict, Optional
from datetime import datetime

logger = logging.getLogger(__name__)


class NotificationManager:
    """
    Manages notifications and alerts for the patching process.
    
    Notification types:
    - Patch failures
    - Manual intervention required
    - Completion summaries
    - Error alerts
    """
    
    def __init__(self, config):
        self.config = config
        self.notifications: List[Dict] = []
    
    def add_notification(
        self,
        level: str,
        title: str,
        message: str,
        vm_name: str = None,
        action_required: bool = False
    ):
        """
        Add a notification.
        
        Args:
            level: Notification level (info, warning, error, critical)
            title: Notification title
            message: Notification message
            vm_name: Optional VM name
            action_required: If True, user action is required
        """
        notification = {
            'timestamp': datetime.now().isoformat(),
            'level': level,
            'title': title,
            'message': message,
            'vm_name': vm_name,
            'action_required': action_required,
            'acknowledged': False
        }
        
        self.notifications.append(notification)
        
        # Log based on level
        log_msg = f"[{level.upper()}] {title}: {message}"
        if vm_name:
            log_msg += f" (VM: {vm_name})"
        
        if level == 'critical' or level == 'error':
            logger.error(log_msg)
        elif level == 'warning':
            logger.warning(log_msg)
        else:
            logger.info(log_msg)
    
    def notify_patch_failure(self, vm_name: str, error: str):
        """Notify about patch failure"""
        self.add_notification(
            level='error',
            title='Patch Failed',
            message=f'Patching failed for {vm_name}: {error}',
            vm_name=vm_name,
            action_required=True
        )
    
    def notify_manual_intervention_required(self, vm_name: str, reason: str):
        """Notify that manual intervention is required"""
        self.add_notification(
            level='warning',
            title='Manual Intervention Required',
            message=f'{vm_name} requires manual intervention: {reason}',
            vm_name=vm_name,
            action_required=True
        )
    
    def notify_vm_skipped(self, vm_name: str, reason: str):
        """Notify that a VM was skipped"""
        self.add_notification(
            level='info',
            title='VM Skipped',
            message=f'{vm_name} was skipped: {reason}',
            vm_name=vm_name,
            action_required=False
        )
    
    def notify_completion(self, summary: Dict):
        """Notify about patching completion with summary"""
        total = summary.get('total_vms', 0)
        success = summary.get('success', 0)
        failed = summary.get('failed', 0)
        skipped = summary.get('skipped', 0)
        
        message = (
            f"Patching cycle completed!\n"
            f"Total VMs: {total}\n"
            f"Success: {success}\n"
            f"Failed: {failed}\n"
            f"Skipped: {skipped}"
        )
        
        level = 'info' if failed == 0 else 'warning'
        
        self.add_notification(
            level=level,
            title='Patching Completed',
            message=message,
            action_required=failed > 0
        )
    
    def get_unacknowledged_notifications(self) -> List[Dict]:
        """Get all unacknowledged notifications"""
        return [n for n in self.notifications if not n['acknowledged']]
    
    def get_action_required_notifications(self) -> List[Dict]:
        """Get notifications requiring user action"""
        return [n for n in self.notifications if n['action_required'] and not n['acknowledged']]
    
    def acknowledge_notification(self, index: int):
        """Mark a notification as acknowledged"""
        if 0 <= index < len(self.notifications):
            self.notifications[index]['acknowledged'] = True
    
    def acknowledge_all(self):
        """Mark all notifications as acknowledged"""
        for notification in self.notifications:
            notification['acknowledged'] = True
    
    def get_summary(self) -> Dict:
        """Get summary of notifications"""
        return {
            'total': len(self.notifications),
            'info': len([n for n in self.notifications if n['level'] == 'info']),
            'warning': len([n for n in self.notifications if n['level'] == 'warning']),
            'error': len([n for n in self.notifications if n['level'] == 'error']),
            'critical': len([n for n in self.notifications if n['level'] == 'critical']),
            'action_required': len(self.get_action_required_notifications()),
            'unacknowledged': len(self.get_unacknowledged_notifications())
        }
    
    def format_for_display(self, notification: Dict) -> str:
        """Format notification for display"""
        lines = [
            f"[{notification['level'].upper()}] {notification['title']}",
            f"Time: {notification['timestamp']}",
        ]
        
        if notification.get('vm_name'):
            lines.append(f"VM: {notification['vm_name']}")
        
        lines.append(f"Message: {notification['message']}")
        
        if notification.get('action_required'):
            lines.append("⚠️ ACTION REQUIRED")
        
        return '\n'.join(lines)
    
    def clear_notifications(self):
        """Clear all notifications"""
        self.notifications.clear()
        logger.info("All notifications cleared")
