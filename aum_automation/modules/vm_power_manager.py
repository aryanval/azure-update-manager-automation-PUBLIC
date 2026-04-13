"""
VM Power Manager
Manages VM power states - tracks initial states, starts/stops VMs
Persists state to JSON for reliability
"""

import os
import json
import logging
from typing import Dict, List
from datetime import datetime
from azure.mgmt.compute import ComputeManagementClient
from azure.core.exceptions import ResourceNotFoundError

logger = logging.getLogger(__name__)


class VMPowerManager:
    """
    Manages VM power states with state persistence.
    
    Features:
    - Tracks initial power states before patching
    - Starts deallocated VMs for patching
    - Restores initial states after patching
    - Persists state to JSON file for recovery
    """
    
    def __init__(self, config, auth_manager, error_handler):
        self.config = config
        self.auth_manager = auth_manager
        self.error_handler = error_handler
        self.compute_client: ComputeManagementClient = None
        self.compute_client_subscription: str = None
        
        # State file path
        self.state_dir = os.path.join(
            os.path.dirname(__file__), 
            '..', 
            'state'
        )
        os.makedirs(self.state_dir, exist_ok=True)
        
        self.state_file = None
        self.initial_states: Dict = {}
    
    def _get_compute_client(self) -> ComputeManagementClient:
        """Get compute management client"""
        current_subscription = self.auth_manager.current_subscription
        if self.compute_client is None or self.compute_client_subscription != current_subscription:
            credential = self.auth_manager.get_credential()
            subscription_id = current_subscription
            
            self.compute_client = ComputeManagementClient(credential, subscription_id)
            self.compute_client_subscription = subscription_id
        
        return self.compute_client
    
    def _get_state_file_path(self, tenant_name: str) -> str:
        """Get state file path for current cycle"""
        timestamp = datetime.now().strftime('%Y%m')
        filename = f"vm_states_{tenant_name}_{timestamp}.json"
        return os.path.join(self.state_dir, filename)
    
    def save_initial_states(self, tenant_name: str, vms: List[Dict]):
        """
        Save initial power states of VMs.
        
        Args:
            tenant_name: Tenant name
            vms: List of VM dictionaries with power_state
        """
        self.state_file = self._get_state_file_path(tenant_name)
        
        self.initial_states = {
            'tenant': tenant_name,
            'timestamp': datetime.now().isoformat(),
            'vms': {}
        }
        
        for vm in vms:
            subscription_id = vm.get('subscription_id', self.auth_manager.current_subscription or 'unknown')
            vm_key = f"{subscription_id}/{vm['resource_group']}/{vm['name']}"
            self.initial_states['vms'][vm_key] = {
                'name': vm['name'],
                'resource_group': vm['resource_group'],
                'subscription_id': subscription_id,
                'initial_power_state': vm.get('power_state', 'unknown'),
                'os_type': vm.get('os_type', 'Unknown'),
                'current_state': vm.get('power_state', 'unknown')
            }
        
        # Save to file
        with open(self.state_file, 'w') as f:
            json.dump(self.initial_states, f, indent=2)
        
        logger.info(f"Saved initial states for {len(vms)} VMs to {self.state_file}")
    
    def load_initial_states(self, tenant_name: str) -> bool:
        """
        Load initial states from file.
        
        Args:
            tenant_name: Tenant name
        
        Returns:
            True if loaded successfully, False otherwise
        """
        self.state_file = self._get_state_file_path(tenant_name)
        
        if not os.path.exists(self.state_file):
            logger.warning(f"State file not found: {self.state_file}")
            return False
        
        try:
            with open(self.state_file, 'r') as f:
                self.initial_states = json.load(f)
            
            logger.info(f"Loaded initial states for {len(self.initial_states.get('vms', {}))} VMs")
            return True
        
        except Exception as e:
            logger.error(f"Error loading state file: {e}")
            return False
    
    def start_vm(self, resource_group: str, vm_name: str, dry_run: bool = False) -> bool:
        """
        Start a deallocated VM.
        
        Args:
            resource_group: Resource group name
            vm_name: VM name
            dry_run: If True, don't actually start VM
        
        Returns:
            True if successful, False otherwise
        """
        if dry_run:
            logger.info(f"[DRY RUN] Would start VM: {vm_name}")
            return True
        
        compute_client = self._get_compute_client()
        
        try:
            logger.info(f"Starting VM: {vm_name}")
            poller = compute_client.virtual_machines.begin_start(resource_group, vm_name)
            
            # Wait for completion
            poller.wait(timeout=300)  # 5 minute timeout
            
            logger.info(f"Successfully started VM: {vm_name}")
            
            # Update state file
            self._update_current_state(resource_group, vm_name, 'running')
            
            return True
        
        except Exception as e:
            error_info = self.error_handler.handle_error(e, vm_name, "start_vm")
            logger.error(f"Failed to start VM: {error_info['message']}")
            return False
    
    def stop_vm(self, resource_group: str, vm_name: str, deallocate: bool = True, dry_run: bool = False) -> bool:
        """
        Stop or deallocate a VM.
        
        Args:
            resource_group: Resource group name
            vm_name: VM name
            deallocate: If True, deallocate (no charges). If False, just stop.
            dry_run: If True, don't actually stop VM
        
        Returns:
            True if successful, False otherwise
        """
        if dry_run:
            action = "deallocate" if deallocate else "stop"
            logger.info(f"[DRY RUN] Would {action} VM: {vm_name}")
            return True
        
        compute_client = self._get_compute_client()
        
        try:
            if deallocate:
                logger.info(f"Deallocating VM: {vm_name}")
                poller = compute_client.virtual_machines.begin_deallocate(resource_group, vm_name)
            else:
                logger.info(f"Stopping VM: {vm_name}")
                poller = compute_client.virtual_machines.begin_power_off(resource_group, vm_name)
            
            # Wait for completion
            poller.wait(timeout=300)  # 5 minute timeout
            
            state = 'deallocated' if deallocate else 'stopped'
            logger.info(f"Successfully {state} VM: {vm_name}")
            
            # Update state file
            self._update_current_state(resource_group, vm_name, state)
            
            return True
        
        except Exception as e:
            error_info = self.error_handler.handle_error(e, vm_name, "stop_vm")
            logger.error(f"Failed to stop VM: {error_info['message']}")
            return False
    
    def _update_current_state(self, resource_group: str, vm_name: str, state: str):
        """Update current state in state file"""
        matched_key = None
        for key, vm_state in self.initial_states.get('vms', {}).items():
            if vm_state.get('resource_group') == resource_group and vm_state.get('name') == vm_name:
                matched_key = key
                break

        if matched_key:
            self.initial_states['vms'][matched_key]['current_state'] = state
            self.initial_states['vms'][matched_key]['last_updated'] = datetime.now().isoformat()
            if self.state_file:
                try:
                    with open(self.state_file, 'w') as f:
                        json.dump(self.initial_states, f, indent=2)
                except Exception as e:
                    logger.error(f"Error saving state file: {e}")
    
    def start_deallocated_vms(self, vms: List[Dict], dry_run: bool = False) -> Dict:
        """
        Start all deallocated VMs (except those marked to skip).
        
        Args:
            vms: List of VM dictionaries
            dry_run: If True, simulate without making changes
        
        Returns:
            Dictionary with results
        """
        results = {
            'started': [],
            'already_running': [],
            'skipped': [],
            'failed': []
        }
        
        for vm in vms:
            # Skip VMs marked to skip
            if vm.get('should_skip', False):
                logger.info(f"Skipping VM (marked to skip): {vm['name']}")
                results['skipped'].append(vm['name'])
                continue
            
            power_state = vm.get('power_state', 'unknown')
            
            if power_state in ['running', 'starting']:
                logger.debug(f"VM already running: {vm['name']}")
                results['already_running'].append(vm['name'])
            
            elif power_state in ['deallocated', 'stopped']:
                vm_sub = vm.get('subscription_id')
                if vm_sub:
                    self.auth_manager.set_subscription(vm_sub)
                if self.start_vm(vm['resource_group'], vm['name'], dry_run):
                    results['started'].append(vm['name'])
                else:
                    results['failed'].append(vm['name'])
            
            else:
                logger.warning(f"VM {vm['name']} in unknown state: {power_state}")
                results['skipped'].append(vm['name'])
        
        logger.info(
            f"Start VMs results: "
            f"{len(results['started'])} started, "
            f"{len(results['already_running'])} already running, "
            f"{len(results['skipped'])} skipped, "
            f"{len(results['failed'])} failed"
        )
        
        return results
    
    def restore_initial_states(self, dry_run: bool = False) -> Dict:
        """
        Restore VMs to their initial power states.
        Only stops VMs that were initially stopped/deallocated.
        
        Args:
            dry_run: If True, simulate without making changes
        
        Returns:
            Dictionary with results
        """
        results = {
            'stopped': [],
            'left_running': [],
            'failed': []
        }
        
        if not self.initial_states:
            logger.warning("No initial states loaded")
            return results
        
        for vm_key, vm_state in self.initial_states.get('vms', {}).items():
            vm_name = vm_state['name']
            resource_group = vm_state['resource_group']
            subscription_id = vm_state.get('subscription_id')
            initial_state = vm_state['initial_power_state']
            
            # If VM was initially deallocated/stopped, restore that state
            if initial_state in ['deallocated', 'stopped']:
                if subscription_id:
                    self.auth_manager.set_subscription(subscription_id)
                if self.stop_vm(resource_group, vm_name, deallocate=True, dry_run=dry_run):
                    results['stopped'].append(vm_name)
                else:
                    results['failed'].append(vm_name)
            else:
                # Leave running VMs as they were
                logger.debug(f"Leaving VM running (was initially running): {vm_name}")
                results['left_running'].append(vm_name)
        
        logger.info(
            f"Restore states results: "
            f"{len(results['stopped'])} stopped, "
            f"{len(results['left_running'])} left running, "
            f"{len(results['failed'])} failed"
        )
        
        return results
    
    def get_state_summary(self) -> Dict:
        """Get summary of current VM states"""
        if not self.initial_states:
            return {'error': 'No states loaded'}
        
        summary = {
            'total_vms': len(self.initial_states.get('vms', {})),
            'initially_running': 0,
            'initially_stopped': 0,
            'currently_running': 0,
            'currently_stopped': 0
        }
        
        for vm_state in self.initial_states.get('vms', {}).values():
            initial = vm_state['initial_power_state']
            current = vm_state.get('current_state', initial)
            
            if initial in ['running', 'starting']:
                summary['initially_running'] += 1
            else:
                summary['initially_stopped'] += 1
            
            if current in ['running', 'starting']:
                summary['currently_running'] += 1
            else:
                summary['currently_stopped'] += 1
        
        return summary
