#!/usr/bin/env python3
"""
Pre-Launch Audit System
Performs security and QA checks before launching the AUM Automation tool
"""

import os
import sys
import yaml
import json
import subprocess
from importlib import metadata as importlib_metadata
from pathlib import Path
from typing import Dict, List, Tuple, Any
import re


class PreLaunchAuditor:
    """Performs comprehensive security and QA checks before launch"""
    
    def __init__(self, project_root: str = "."):
        self.project_root = Path(project_root)
        self.issues = []
        self.warnings = []
        self.checks_passed = 0
        self.checks_failed = 0
        
    def print_header(self, text: str):
        """Print a formatted header"""
        print(f"\n{'='*60}")
        print(f"  {text}")
        print(f"{'='*60}")
    
    def print_check(self, name: str, status: str, details: str = ""):
        """Print a check result"""
        symbols = {
            "PASS": "✓",
            "FAIL": "✗",
            "WARN": "⚠",
            "INFO": "ℹ"
        }
        colors = {
            "PASS": "\033[92m",  # Green
            "FAIL": "\033[91m",  # Red
            "WARN": "\033[93m",  # Yellow
            "INFO": "\033[94m",  # Blue
        }
        reset = "\033[0m"
        
        symbol = symbols.get(status, "•")
        color = colors.get(status, "")
        
        print(f"{color}{symbol} {name}{reset}")
        if details:
            print(f"  {details}")
    
    def check_python_version(self) -> bool:
        """Check Python version meets requirements"""
        self.print_check("Checking Python version", "INFO")
        
        version = sys.version_info
        if version.major == 3 and version.minor >= 9:
            self.print_check(f"Python {version.major}.{version.minor}.{version.micro}", "PASS", 
                           "Meets minimum requirement (3.9+)")
            self.checks_passed += 1
            return True
        else:
            self.print_check(f"Python {version.major}.{version.minor}.{version.micro}", "FAIL",
                           "Requires Python 3.9 or higher")
            self.issues.append("Python version too old")
            self.checks_failed += 1
            return False
    
    def check_azure_cli(self) -> bool:
        """Check Azure CLI installation and authentication"""
        self.print_check("Checking Azure CLI", "INFO")
        
        try:
            # Check if az command exists
            result = subprocess.run(["az", "--version"], 
                                  capture_output=True, text=True, timeout=5)
            if result.returncode == 0:
                self.print_check("Azure CLI installed", "PASS")
                self.checks_passed += 1
                
                # Check authentication
                auth_result = subprocess.run(["az", "account", "show"],
                                           capture_output=True, text=True, timeout=10)
                if auth_result.returncode == 0:
                    account_info = json.loads(auth_result.stdout)
                    self.print_check("Azure CLI authenticated", "PASS",
                                   f"Account: {account_info.get('name', 'Unknown')}")
                    self.checks_passed += 1
                    return True
                else:
                    self.print_check("Azure CLI not authenticated", "WARN",
                                   "Run 'az login' before patching")
                    self.warnings.append("Azure CLI not authenticated")
                    self.checks_failed += 1
                    return False
            else:
                self.print_check("Azure CLI not found", "FAIL")
                self.issues.append("Azure CLI not installed")
                self.checks_failed += 1
                return False
                
        except (subprocess.TimeoutExpired, FileNotFoundError, Exception) as e:
            self.print_check("Azure CLI check failed", "FAIL", str(e))
            self.issues.append(f"Azure CLI error: {e}")
            self.checks_failed += 1
            return False
    
    def check_config_file(self) -> bool:
        """Check configuration file exists and is valid"""
        self.print_check("Checking configuration file", "INFO")
        
        config_path = self.project_root / "config" / "config.yaml"
        
        if not config_path.exists():
            self.print_check("config.yaml not found", "FAIL",
                           "Copy config.example.yaml to config.yaml")
            self.issues.append("Missing config.yaml")
            self.checks_failed += 1
            return False
        
        self.print_check("config.yaml exists", "PASS")
        self.checks_passed += 1
        
        # Validate YAML syntax
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            
            self.print_check("YAML syntax valid", "PASS")
            self.checks_passed += 1
            
            # Check for required fields
            if not config.get('tenants'):
                self.print_check("No tenants configured", "FAIL")
                self.issues.append("config.yaml missing 'tenants' section")
                self.checks_failed += 1
                return False
            
            # Check for placeholder subscription IDs
            has_placeholders = False
            for tenant_name, tenant_config in config.get('tenants', {}).items():
                sub_id = tenant_config.get('subscription_id', '')
                if not sub_id or sub_id in ['YOUR-SUBSCRIPTION-ID', '', 'PLACEHOLDER']:
                    self.print_check(f"Tenant '{tenant_name}' has placeholder ID", "WARN")
                    self.warnings.append(f"Tenant {tenant_name} needs real subscription ID")
                    has_placeholders = True
            
            if not has_placeholders:
                self.print_check("All tenants have subscription IDs", "PASS")
                self.checks_passed += 1
            else:
                self.checks_failed += 1
            
            return True
            
        except yaml.YAMLError as e:
            self.print_check("Invalid YAML syntax", "FAIL", str(e))
            self.issues.append(f"config.yaml syntax error: {e}")
            self.checks_failed += 1
            return False
        except Exception as e:
            self.print_check("Config validation error", "FAIL", str(e))
            self.issues.append(f"Config error: {e}")
            self.checks_failed += 1
            return False
    
    def check_directory_structure(self) -> bool:
        """Check required directories exist"""
        self.print_check("Checking directory structure", "INFO")
        
        required_dirs = ['config', 'modules', 'logs', 'reports', 'state']
        all_exist = True
        
        for dirname in required_dirs:
            dir_path = self.project_root / dirname
            if dir_path.exists():
                self.print_check(f"Directory '{dirname}' exists", "PASS")
                self.checks_passed += 1
            else:
                self.print_check(f"Directory '{dirname}' missing", "WARN",
                               "Will be created automatically")
                self.warnings.append(f"Missing directory: {dirname}")
                # Create missing directories
                try:
                    dir_path.mkdir(parents=True, exist_ok=True)
                    self.print_check(f"Created '{dirname}' directory", "INFO")
                except Exception as e:
                    self.print_check(f"Failed to create '{dirname}'", "FAIL", str(e))
                    all_exist = False
                    self.checks_failed += 1
        
        return all_exist
    
    def check_dependencies(self) -> bool:
        """Check Python dependencies are installed"""
        self.print_check("Checking Python dependencies", "INFO")
        
        requirements_path = self.project_root / "requirements.txt"
        
        if not requirements_path.exists():
            self.print_check("requirements.txt not found", "FAIL")
            self.issues.append("Missing requirements.txt")
            self.checks_failed += 1
            return False
        
        # Read required packages
        required_packages = []
        try:
            with open(requirements_path, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        if line.startswith('-'):
                            continue
                        req = line.split(';', 1)[0].strip()
                        pkg_name = re.split(r'[<>=!~]', req, maxsplit=1)[0].strip()
                        pkg_name = pkg_name.split('[', 1)[0].strip()
                        if pkg_name:
                            required_packages.append(pkg_name)
        except Exception as e:
            self.print_check("Failed to read requirements.txt", "FAIL", str(e))
            self.checks_failed += 1
            return False
        
        # Check each package
        missing_packages = []
        for pkg in required_packages:
            try:
                importlib_metadata.version(pkg)
            except importlib_metadata.PackageNotFoundError:
                missing_packages.append(pkg)
        
        if missing_packages:
            self.print_check(f"{len(missing_packages)} packages missing", "WARN",
                           f"Missing: {', '.join(missing_packages[:3])}...")
            self.warnings.append(f"Missing packages: {', '.join(missing_packages)}")
            self.checks_failed += 1
            return False
        else:
            self.print_check("All dependencies installed", "PASS")
            self.checks_passed += 1
            return True
    
    def check_sensitive_data(self) -> bool:
        """Check for exposed sensitive data in logs and state files"""
        self.print_check("Checking for exposed credentials", "INFO")
        
        sensitive_patterns = [
            (r'password\s*[:=]\s*["\']?[^"\'\s]+', "password"),
            (r'client_secret\s*[:=]\s*["\']?[^"\'\s]+', "client_secret"),
            (r'api[_-]?key\s*[:=]\s*["\']?[^"\'\s]+', "api_key"),
            (r'secret\s*[:=]\s*["\']?[^"\'\s]+', "secret"),
            (r'token\s*[:=]\s*["\']?[A-Za-z0-9+/=]{20,}', "token"),
        ]
        
        files_to_check = []
        
        # Check logs directory
        logs_dir = self.project_root / "logs"
        if logs_dir.exists():
            files_to_check.extend(logs_dir.glob("*.log"))
        
        # Check state directory
        state_dir = self.project_root / "state"
        if state_dir.exists():
            files_to_check.extend(state_dir.glob("*.json"))
        
        found_sensitive = False
        
        for file_path in files_to_check:
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    for pattern, data_type in sensitive_patterns:
                        if re.search(pattern, content, re.IGNORECASE):
                            self.print_check(f"Possible {data_type} in {file_path.name}", "WARN")
                            self.warnings.append(f"Sensitive data may be in {file_path.name}")
                            found_sensitive = True
            except Exception:
                pass
        
        if not found_sensitive:
            self.print_check("No exposed credentials found", "PASS")
            self.checks_passed += 1
        else:
            self.checks_failed += 1
        
        return not found_sensitive
    
    def check_gitignore(self) -> bool:
        """Check .gitignore includes sensitive files"""
        self.print_check("Checking .gitignore configuration", "INFO")
        
        gitignore_path = self.project_root / ".gitignore"
        
        if not gitignore_path.exists():
            self.print_check(".gitignore not found", "WARN",
                           "Sensitive files may be committed")
            self.warnings.append("Missing .gitignore")
            self.checks_failed += 1
            return False
        
        try:
            with open(gitignore_path, 'r') as f:
                gitignore_content = f.read()
            
            critical_patterns = [
                'config.yaml',
                '*.log',
                'venv/',
                '__pycache__',
            ]
            
            missing_patterns = []
            for pattern in critical_patterns:
                if pattern not in gitignore_content:
                    missing_patterns.append(pattern)
            
            if missing_patterns:
                self.print_check("Incomplete .gitignore", "WARN",
                               f"Missing: {', '.join(missing_patterns)}")
                self.warnings.append("Incomplete .gitignore configuration")
                self.checks_failed += 1
                return False
            else:
                self.print_check(".gitignore properly configured", "PASS")
                self.checks_passed += 1
                return True
                
        except Exception as e:
            self.print_check("Failed to read .gitignore", "WARN", str(e))
            self.warnings.append(".gitignore read error")
            self.checks_failed += 1
            return False
    
    def check_permissions(self) -> bool:
        """Check file permissions for security"""
        self.print_check("Checking file permissions", "INFO")
        
        config_path = self.project_root / "config" / "config.yaml"
        
        if not config_path.exists():
            return True  # Already checked in config check
        
        try:
            stat_info = config_path.stat()
            mode = oct(stat_info.st_mode)[-3:]
            
            # Check if world-readable (last digit should be 0 or 4)
            if mode[-1] in ['6', '7']:
                self.print_check("config.yaml is world-writable", "WARN",
                               f"Permissions: {mode} - Consider: chmod 600")
                self.warnings.append("config.yaml has overly permissive permissions")
                self.checks_failed += 1
                return False
            else:
                self.print_check(f"config.yaml permissions OK ({mode})", "PASS")
                self.checks_passed += 1
                return True
                
        except Exception as e:
            self.print_check("Permission check failed", "INFO", str(e))
            return True
    
    def check_module_integrity(self) -> bool:
        """Check all required modules are present"""
        self.print_check("Checking module integrity", "INFO")
        
        required_modules = [
            'auth_manager.py',
            'config.py',
            'error_handler.py',
            'excel_tracker.py',
            'vm_inventory.py',
            'vm_power_manager.py',
            'aum_manager.py',
            'patch_executor.py',
            'notifications.py',
        ]
        
        modules_dir = self.project_root / "modules"
        missing_modules = []
        
        for module in required_modules:
            if not (modules_dir / module).exists():
                missing_modules.append(module)
        
        if missing_modules:
            self.print_check(f"{len(missing_modules)} modules missing", "FAIL",
                           f"Missing: {', '.join(missing_modules)}")
            self.issues.append(f"Missing modules: {', '.join(missing_modules)}")
            self.checks_failed += 1
            return False
        else:
            self.print_check("All modules present", "PASS")
            self.checks_passed += 1
            return True
    
    def check_state_files(self) -> bool:
        """Check state files for corruption"""
        self.print_check("Checking state files", "INFO")
        
        state_dir = self.project_root / "state"
        if not state_dir.exists():
            self.print_check("No state files to check", "INFO")
            return True
        
        state_files = list(state_dir.glob("*.json"))
        
        if not state_files:
            self.print_check("No state files found", "INFO")
            return True
        
        corrupted = []
        for state_file in state_files:
            try:
                with open(state_file, 'r') as f:
                    json.load(f)
            except json.JSONDecodeError:
                corrupted.append(state_file.name)
        
        if corrupted:
            self.print_check(f"{len(corrupted)} corrupted state files", "WARN",
                           f"Files: {', '.join(corrupted)}")
            self.warnings.append(f"Corrupted state files: {', '.join(corrupted)}")
            self.checks_failed += 1
            return False
        else:
            self.print_check(f"All {len(state_files)} state files valid", "PASS")
            self.checks_passed += 1
            return True
    
    def check_disk_space(self) -> bool:
        """Check available disk space"""
        self.print_check("Checking disk space", "INFO")
        
        try:
            stat = os.statvfs(self.project_root)
            free_space_mb = (stat.f_bavail * stat.f_frsize) / (1024 * 1024)
            
            if free_space_mb < 100:
                self.print_check(f"Low disk space: {free_space_mb:.0f} MB", "WARN",
                               "Recommend at least 100 MB free")
                self.warnings.append(f"Low disk space: {free_space_mb:.0f} MB")
                self.checks_failed += 1
                return False
            else:
                self.print_check(f"Disk space OK: {free_space_mb:.0f} MB free", "PASS")
                self.checks_passed += 1
                return True
                
        except Exception as e:
            self.print_check("Disk space check skipped", "INFO", str(e))
            return True
    
    def run_all_checks(self) -> bool:
        """Run all pre-launch checks"""
        self.print_header("AUM Automation Pre-Launch Audit")
        
        print("\nPerforming security and QA checks...")
        print("This ensures the tool is safe to run.\n")
        
        # Run all checks
        checks = [
            ("System Requirements", [
                self.check_python_version,
                self.check_azure_cli,
                self.check_dependencies,
            ]),
            ("Configuration & Files", [
                self.check_config_file,
                self.check_directory_structure,
                self.check_module_integrity,
            ]),
            ("Security", [
                self.check_sensitive_data,
                self.check_gitignore,
                self.check_permissions,
            ]),
            ("Data Integrity", [
                self.check_state_files,
                self.check_disk_space,
            ]),
        ]
        
        for category, check_functions in checks:
            self.print_header(category)
            for check_func in check_functions:
                try:
                    check_func()
                except Exception as e:
                    self.print_check(f"{check_func.__name__} failed", "FAIL", str(e))
                    self.issues.append(f"Unexpected error in {check_func.__name__}: {e}")
                    self.checks_failed += 1
        
        # Print summary
        self.print_summary()
        
        # Return True only if no critical issues
        return len(self.issues) == 0
    
    def print_summary(self):
        """Print audit summary"""
        self.print_header("Audit Summary")
        
        print(f"\n✓ Checks passed: {self.checks_passed}")
        print(f"✗ Checks failed: {self.checks_failed}")
        print(f"⚠ Warnings: {len(self.warnings)}")
        
        if self.issues:
            print(f"\n\033[91mCRITICAL ISSUES ({len(self.issues)}):\033[0m")
            for i, issue in enumerate(self.issues, 1):
                print(f"  {i}. {issue}")
        
        if self.warnings:
            print(f"\n\033[93mWARNINGS ({len(self.warnings)}):\033[0m")
            for i, warning in enumerate(self.warnings, 1):
                print(f"  {i}. {warning}")
        
        if not self.issues and not self.warnings:
            print(f"\n\033[92m✓ All checks passed! System is ready.\033[0m")
        elif not self.issues:
            print(f"\n\033[93m⚠ System is functional but has warnings.\033[0m")
        else:
            print(f"\n\033[91m✗ Critical issues found. Please fix before proceeding.\033[0m")
        
        print()


def run_audit(project_root: str = ".") -> bool:
    """
    Run pre-launch audit and return True if safe to proceed
    """
    auditor = PreLaunchAuditor(project_root)
    return auditor.run_all_checks()


if __name__ == "__main__":
    # Run audit when called directly
    success = run_audit()
    sys.exit(0 if success else 1)
