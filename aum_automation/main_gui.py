"""
Azure Update Manager Automation - Main GUI Application
Desktop application for managing Azure patching across tenants
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog
import threading
import logging
import os
import sys
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

# Add modules to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'modules'))

from modules.config import get_config
from modules.auth_manager import AzureAuthManager
from modules.error_handler import ErrorHandler
from modules.excel_tracker import ExcelTracker
from modules.vm_inventory import VMInventory
from modules.vm_power_manager import VMPowerManager
from modules.aum_manager import AUMManager
from modules.patch_executor import PatchExecutor
from modules.notifications import NotificationManager
from modules.resource_graph_exporter import ResourceGraphExporter
from modules.db_manager import InventoryDB, new_run_id
from modules.health_gate import HealthGate

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/aum_automation.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


class AUMAutomationGUI:
    """Main GUI application for AUM automation"""
    
    def __init__(self, root):
        self.root = root
        self.root.title("Azure Update Manager Automation")
        self.root.geometry("1400x900")
        
        # Dark mode colors - Black and Blue only
        self.colors = {
            'bg_dark': '#000000',      # Pure black
            'bg_medium': '#0A0A0A',    # Very dark gray
            'bg_light': '#1A1A1A',     # Dark gray
            'blue': '#0078D4',         # Azure blue
            'blue_light': '#4A9EFF',   # Light blue
            'text': '#FFFFFF',         # White text
            'text_dim': '#B0B0B0'      # Dimmed text
        }
        
        # Configure dark theme
        self.setup_dark_theme()
        
        # Load configuration
        try:
            self.config = get_config()
            logger.info("Configuration loaded successfully")
        except Exception as e:
            messagebox.showerror("Configuration Error", f"Failed to load configuration: {e}")
            sys.exit(1)
        
        # Initialize managers
        self.auth_manager = AzureAuthManager(self.config)
        self.error_handler = ErrorHandler(self.config)
        self.excel_tracker = ExcelTracker(self.config)
        self.notification_manager = NotificationManager(self.config)
        self.resource_graph_exporter = ResourceGraphExporter(self.config, self.auth_manager)
        
        # Local inventory DB (gitignored, never transmitted)
        try:
            self.db = InventoryDB()
            logger.info(f"Local inventory DB: {self.db.get_db_path()}")
        except Exception as e:
            logger.warning(f"Could not initialise local DB (non-fatal): {e}")
            self.db = None

        # Will be initialized after authentication
        self.vm_inventory = None
        self.power_manager = None
        self.aum_manager = None
        self.patch_executor = None
        self.health_gate = None

        # State variables
        self.current_tenant = tk.StringVar()
        self.current_subscription_choice = tk.StringVar()
        self.vms = []
        self.selected_vms = []
        self.user_exclusions = []
        self.cancel_update_check = False
        self.is_checking_updates = False

        # Multi-tenant dashboard state  {tenant_name -> {label_vars, ...}}
        self._dashboard_widgets: dict = {}
        self._cancel_all_flag = [False]   # mutable so threads can read it
        
        # Setup UI
        self.setup_ui()
        
        logger.info("AUM Automation GUI initialized")
    
    def setup_dark_theme(self):
        """Configure dark mode theme with black and blue"""
        style = ttk.Style()
        
        # Configure root window
        self.root.configure(bg=self.colors['bg_dark'])
        
        # Configure ttk styles
        style.configure('.',
            background=self.colors['bg_dark'],
            foreground=self.colors['text'],
            fieldbackground=self.colors['bg_light'],
            bordercolor=self.colors['blue'],
            darkcolor=self.colors['bg_dark'],
            lightcolor=self.colors['bg_light']
        )
        
        style.configure('TFrame',
            background=self.colors['bg_dark']
        )
        
        style.configure('TLabel',
            background=self.colors['bg_dark'],
            foreground=self.colors['text']
        )
        
        style.configure('TButton',
            background=self.colors['blue'],
            foreground=self.colors['text'],
            borderwidth=0,
            focuscolor='none',
            padding=6
        )
        
        style.map('TButton',
            background=[('active', self.colors['blue_light'])],
            foreground=[('active', self.colors['text'])]
        )
        
        style.configure('Treeview',
            background=self.colors['bg_light'],
            foreground=self.colors['text'],
            fieldbackground=self.colors['bg_light'],
            borderwidth=0
        )
        
        style.configure('Treeview.Heading',
            background=self.colors['blue'],
            foreground=self.colors['text'],
            borderwidth=0
        )
        
        style.map('Treeview.Heading',
            background=[('active', self.colors['blue_light'])]
        )
    
    def setup_ui(self):
        """Setup the user interface"""
        # Top frame - Tenant selection and authentication
        top_frame = ttk.Frame(self.root, padding="10")
        top_frame.grid(row=0, column=0, sticky=(tk.W, tk.E))
        
        ttk.Label(top_frame, text="Select Tenant:").grid(row=0, column=0, padx=5)
        
        tenant_combo = ttk.Combobox(
            top_frame, 
            textvariable=self.current_tenant,
            values=self.config.get_tenant_list(),
            state='readonly',
            width=18
        )
        tenant_combo.grid(row=0, column=1, padx=5)
        tenant_combo.bind("<<ComboboxSelected>>", self.on_tenant_selected)

        ttk.Label(top_frame, text="Subscription:").grid(row=0, column=2, padx=5)
        self.subscription_combo = ttk.Combobox(
            top_frame,
            textvariable=self.current_subscription_choice,
            values=[],
            state='readonly',
            width=30
        )
        self.subscription_combo.grid(row=0, column=3, padx=5)
        
        ttk.Button(
            top_frame,
            text="Login to Azure",
            command=self.show_login_dialog
        ).grid(row=0, column=4, padx=5)

        ttk.Button(
            top_frame,
            text="Authenticate",
            command=self.authenticate_tenant
        ).grid(row=0, column=5, padx=5)

        self.auth_status_label = ttk.Label(top_frame, text="Not authenticated", foreground="red")
        self.auth_status_label.grid(row=0, column=6, padx=5)

        ttk.Button(
            top_frame,
            text="Refresh Inventory",
            command=self.refresh_inventory
        ).grid(row=0, column=7, padx=5)

        # Create notebook (tabs)
        notebook = ttk.Notebook(self.root)
        notebook.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S), padx=10, pady=10)
        
        # Configure grid weight
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=1)
        
        # Tab 1: VM Inventory
        self.setup_inventory_tab(notebook)

        # Tab 2: Patch Management
        self.setup_patch_tab(notebook)

        # Tab 3: Monitoring
        self.setup_monitoring_tab(notebook)

        # Tab 4: All-Tenant Dashboard
        self.setup_dashboard_tab(notebook)

        # Tab 5: Logs
        self.setup_logs_tab(notebook)
        
        # Bottom frame - Progress and status
        self.setup_status_frame()

    def on_tenant_selected(self, _event=None):
        """Populate subscription options for selected tenant"""
        tenant = self.current_tenant.get()
        if not tenant:
            return

        try:
            sub_ids = self.config.get_subscription_ids(tenant)
        except Exception:
            sub_ids = []

        if len(sub_ids) > 1:
            combo_values = ["ALL subscriptions"] + sub_ids
            self.subscription_combo.configure(values=combo_values)
            self.current_subscription_choice.set("ALL subscriptions")
        elif len(sub_ids) == 1:
            self.subscription_combo.configure(values=sub_ids)
            self.current_subscription_choice.set(sub_ids[0])
        else:
            self.subscription_combo.configure(values=[])
            self.current_subscription_choice.set("")

    def _get_selected_subscription_ids(self):
        """Get selected subscription IDs from UI selection."""
        tenant = self.current_tenant.get()
        all_subs = self.config.get_subscription_ids(tenant)
        selected = self.current_subscription_choice.get().strip()

        if selected == "ALL subscriptions":
            return all_subs
        if selected:
            return [selected]
        return []
        
    
    def setup_inventory_tab(self, notebook):
        """Setup VM inventory tab"""
        inventory_frame = ttk.Frame(notebook, padding="10")
        notebook.add(inventory_frame, text="VM Inventory")
        
        # VM list with scrollbar
        columns = ('Name', 'Resource Group', 'OS Type', 'Power State', 'Type', 'Status', 'Last Checked')
        self.vm_tree = ttk.Treeview(inventory_frame, columns=columns, show='headings', height=20)
        
        # Set column widths
        column_widths = {
            'Name': 150,
            'Resource Group': 150,
            'OS Type': 100,
            'Power State': 100,
            'Type': 80,
            'Status': 100,
            'Last Checked': 150
        }
        
        for col in columns:
            self.vm_tree.heading(col, text=col)
            self.vm_tree.column(col, width=column_widths.get(col, 150))
        
        scrollbar = ttk.Scrollbar(inventory_frame, orient=tk.VERTICAL, command=self.vm_tree.yview)
        self.vm_tree.configure(yscroll=scrollbar.set)
        
        self.vm_tree.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        scrollbar.grid(row=0, column=1, sticky=(tk.N, tk.S))
        
        # Buttons
        button_frame = ttk.Frame(inventory_frame)
        button_frame.grid(row=1, column=0, pady=10, sticky=tk.W)
        
        ttk.Button(button_frame, text="Start Deallocated VMs", command=self.start_deallocated_vms).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Run Health Gate", command=self.run_health_gate).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Check for Updates", command=self.check_for_updates).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Stop Check", command=self.stop_check_for_updates).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            button_frame,
            text="STOP All VMs We Started",
            command=self.stop_started_vms
        ).pack(side=tk.LEFT, padx=5)
        
        # Azure Resource Graph Export buttons (in a separate row)
        export_frame = ttk.Frame(inventory_frame)
        export_frame.grid(row=2, column=0, pady=5, sticky=tk.W)
        
        ttk.Label(export_frame, text="Azure Resource Graph Exports:").pack(side=tk.LEFT, padx=5)
        ttk.Button(
            export_frame, 
            text="Export Asset Inventory (Saved Query)", 
            command=self.export_asset_inventory
        ).pack(side=tk.LEFT, padx=5)
        ttk.Button(
            export_frame, 
            text="Export Pre-Patch Data (Saved Query)", 
            command=self.export_pre_patch_data
        ).pack(side=tk.LEFT, padx=5)
        
        inventory_frame.columnconfigure(0, weight=1)
        inventory_frame.rowconfigure(0, weight=1)
    
    def setup_patch_tab(self, notebook):
        """Setup patch management tab"""
        patch_frame = ttk.Frame(notebook, padding="10")
        notebook.add(patch_frame, text="Patch Management")
        
        # Instructions
        ttk.Label(
            patch_frame,
            text="Configure patch exclusions and execute patching",
            font=('Arial', 12, 'bold')
        ).grid(row=0, column=0, columnspan=2, pady=10)
        
        # Exclusions
        ttk.Label(patch_frame, text="Additional Exclusions (one per line):").grid(row=1, column=0, sticky=tk.W, pady=5)
        
        self.exclusions_text = scrolledtext.ScrolledText(patch_frame, width=50, height=10)
        self.exclusions_text.grid(row=2, column=0, columnspan=2, pady=5, sticky=(tk.W, tk.E))
        
        # Always excluded (readonly)
        ttk.Label(patch_frame, text="Always Excluded (SQL, etc.):").grid(row=3, column=0, sticky=tk.W, pady=5)
        
        always_excluded = ttk.Label(
            patch_frame,
            text=", ".join(self.config.get_always_exclude_patches()),
            wraplength=500
        )
        always_excluded.grid(row=4, column=0, columnspan=2, sticky=tk.W)
        
        # Patch execution buttons
        button_frame = ttk.Frame(patch_frame)
        button_frame.grid(row=5, column=0, columnspan=2, pady=20)
        
        ttk.Button(
            button_frame,
            text="Execute Patches (AUM)",
            command=self.execute_patches_aum,
            width=25
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="Patch Unsupported VMs",
            command=self.show_unsupported_menu,
            width=25
        ).pack(side=tk.LEFT, padx=5)
        
        patch_frame.columnconfigure(0, weight=1)
    
    def setup_monitoring_tab(self, notebook):
        """Setup monitoring tab"""
        monitor_frame = ttk.Frame(notebook, padding="10")
        notebook.add(monitor_frame, text="Monitoring")
        
        ttk.Label(
            monitor_frame,
            text="Real-time Patch Status",
            font=('Arial', 12, 'bold')
        ).grid(row=0, column=0, pady=10)
        
        # Status text area
        self.status_text = scrolledtext.ScrolledText(monitor_frame, width=100, height=25)
        self.status_text.grid(row=1, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Buttons
        button_frame = ttk.Frame(monitor_frame)
        button_frame.grid(row=2, column=0, pady=10)
        
        ttk.Button(button_frame, text="Update Excel", command=self.update_excel).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Generate Summary", command=self.generate_summary).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="🛑 STOP All Started VMs", command=self.restore_vm_states).pack(side=tk.LEFT, padx=5)
        
        monitor_frame.columnconfigure(0, weight=1)
        monitor_frame.rowconfigure(1, weight=1)
    
    def setup_logs_tab(self, notebook):
        """Setup logs tab"""
        logs_frame = ttk.Frame(notebook, padding="10")
        notebook.add(logs_frame, text="Logs")
        
        self.logs_text = scrolledtext.ScrolledText(logs_frame, width=100, height=30)
        self.logs_text.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        
        # Auto-scroll checkbox
        self.auto_scroll = tk.BooleanVar(value=True)
        ttk.Checkbutton(logs_frame, text="Auto-scroll", variable=self.auto_scroll).grid(row=1, column=0, sticky=tk.W)
        
        logs_frame.columnconfigure(0, weight=1)
        logs_frame.rowconfigure(0, weight=1)
    
    def setup_status_frame(self):
        """Setup status bar at bottom"""
        status_frame = ttk.Frame(self.root, padding="5")
        status_frame.grid(row=2, column=0, sticky=(tk.W, tk.E))
        
        self.progress = ttk.Progressbar(status_frame, mode='indeterminate')
        self.progress.grid(row=0, column=0, sticky=(tk.W, tk.E), padx=5)
        
        self.status_label = ttk.Label(status_frame, text="Ready")
        self.status_label.grid(row=0, column=1, padx=5)
        
        status_frame.columnconfigure(0, weight=1)
    
    def log_message(self, message, level='INFO'):
        """Add message to logs tab"""
        timestamp = datetime.now().strftime('%H:%M:%S')
        log_entry = f"[{timestamp}] [{level}] {message}\n"
        
        self.logs_text.insert(tk.END, log_entry)
        
        if self.auto_scroll.get():
            self.logs_text.see(tk.END)
        
        logger.info(message)
    
    def update_status(self, message):
        """Update status bar"""
        self.status_label.config(text=message)
        self.log_message(message)
    
    def authenticate_tenant(self):
        """Authenticate for selected tenant"""
        tenant = self.current_tenant.get()
        
        if not tenant:
            messagebox.showwarning("Warning", "Please select a tenant first")
            return
        
        selected_sub = self.current_subscription_choice.get().strip()
        if not selected_sub:
            messagebox.showwarning("Warning", "Please select a subscription option")
            return

        self.update_status(f"Authenticating for {tenant}...")

        # Run in thread to avoid blocking UI
        def auth_thread():
            if selected_sub == "ALL subscriptions":
                success = self.auth_manager.authenticate_for_tenant(tenant)
            else:
                success = self.auth_manager.authenticate_for_tenant_subscription(tenant, selected_sub)

            if success:
                self.root.after(0, lambda: self.on_auth_success(tenant))
            else:
                self.root.after(0, self.on_auth_failure)
        
        threading.Thread(target=auth_thread, daemon=True).start()

    def _get_scope_label(self):
        selected = self.current_subscription_choice.get().strip()
        if selected == "ALL subscriptions":
            return f"{self.current_tenant.get()} (ALL subscriptions)"
        return f"{self.current_tenant.get()} ({selected[:8]}...)"
    
    def on_auth_success(self, tenant):
        """Handle successful authentication"""
        self.auth_status_label.config(text=f"✓ Authenticated: {self._get_scope_label()}", foreground="green")
        self.update_status(f"Successfully authenticated for {self._get_scope_label()}")
        
        # Initialize tenant-specific managers
        self.vm_inventory = VMInventory(self.config, self.auth_manager, self.error_handler, db=self.db)
        self.power_manager = VMPowerManager(self.config, self.auth_manager, self.error_handler)
        self.aum_manager = AUMManager(self.config, self.auth_manager, self.error_handler)
        self.patch_executor = PatchExecutor(self.config, self.auth_manager, self.aum_manager, self.error_handler)
        self.health_gate = HealthGate(self.auth_manager, self.error_handler)
        
        # Get current account
        current_user = self.auth_manager.get_current_user()
        
        # Check for eligible PIM roles first
        self.update_status("Checking for eligible PIM roles...")
        self.log_message("Checking for eligible PIM roles...")
        
        # Get detailed list of eligible roles
        import subprocess
        import json
        try:
            result = subprocess.run(
                ['az', 'rest', '--method', 'GET',
                 '--url', 'https://management.azure.com/providers/Microsoft.Authorization/roleEligibilityScheduleInstances?api-version=2020-10-01&$filter=asTarget()'],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode == 0:
                response = json.loads(result.stdout)
                eligible_roles = response.get('value', [])
                
                # Extract role names
                role_names = []
                for role in eligible_roles:
                    props = role.get('properties', {})
                    expanded = props.get('expandedProperties', {})
                    role_def = expanded.get('roleDefinition', {})
                    display_name = role_def.get('displayName', 'Unknown Role')
                    scope_type = props.get('scope', '').split('/')
                    if len(scope_type) > 1:
                        scope_level = scope_type[1]  # 'subscriptions', 'resourceGroups', etc.
                    else:
                        scope_level = 'Unknown'
                    role_names.append(f"{display_name} ({scope_level})")
                
                num_eligible = len(eligible_roles)
            else:
                num_eligible = 0
                role_names = []
                eligible_roles = []
        except Exception as e:
            num_eligible = 0
            role_names = []
            eligible_roles = []
            self.log_message(f"Error checking PIM roles: {e}")
        
        if num_eligible == 0:
            # No eligible roles
            response = messagebox.askyesno(
                "No PIM Roles Found",
                f"Logged in as: {current_user}\n"
                f"Tenant: {tenant}\n\n"
                "⚠️ No eligible PIM roles were found.\n\n"
                "This might mean:\n"
                "  • You don't have PIM-eligible roles for this tenant\n"
                "  • You're logged in with the wrong account\n\n"
                "Do you want to continue anyway?"
            )
            
            if response:
                self.refresh_inventory()
            else:
                messagebox.showinfo(
                    "Info", 
                    "Please:\n"
                    "1. Run: az logout\n"
                    "2. Login with correct account for this tenant\n"
                    "3. Try authenticating again"
                )
            return
        
        # Show PIM roles dialog
        dialog = tk.Toplevel(self.root)
        dialog.title("PIM Role Activation Required")
        dialog.geometry("600x500")
        dialog.transient(self.root)
        dialog.grab_set()
        
        # Center the dialog
        dialog.update_idletasks()
        x = (dialog.winfo_screenwidth() // 2) - (300)
        y = (dialog.winfo_screenheight() // 2) - (250)
        dialog.geometry(f"+{x}+{y}")
        
        user_choice = {'value': None}
        
        # Header
        header_frame = ttk.Frame(dialog, padding=15)
        header_frame.pack(fill='x')
        
        ttk.Label(
            header_frame,
            text=f"Account: {current_user}",
            font=('Arial', 9)
        ).pack(anchor='w')
        
        ttk.Label(
            header_frame,
            text=f"Tenant: {tenant}",
            font=('Arial', 9)
        ).pack(anchor='w')
        
        ttk.Separator(dialog, orient='horizontal').pack(fill='x', pady=10)
        
        # Roles list
        roles_frame = ttk.Frame(dialog, padding=15)
        roles_frame.pack(fill='both', expand=True)
        
        ttk.Label(
            roles_frame,
            text=f"Found {num_eligible} Eligible PIM Role(s):",
            font=('Arial', 11, 'bold')
        ).pack(pady=(0, 10))
        
        # Scrollable list of roles
        roles_text = scrolledtext.ScrolledText(
            roles_frame,
            height=8,
            width=60,
            font=('Courier', 10),
            wrap=tk.WORD
        )
        roles_text.pack(fill='both', expand=True)
        
        for i, role_name in enumerate(role_names, 1):
            roles_text.insert(tk.END, f"  {i}. {role_name}\n")
        
        roles_text.config(state=tk.DISABLED)
        
        # Instructions
        ttk.Label(
            roles_frame,
            text="\nThese roles must be activated to perform patching operations.",
            font=('Arial', 9)
        ).pack(pady=(10, 0))
        
        ttk.Separator(dialog, orient='horizontal').pack(fill='x', pady=10)
        
        # Buttons
        btn_frame = ttk.Frame(dialog, padding=15)
        btn_frame.pack(fill='x')
        
        def on_open_portal():
            user_choice['value'] = 'portal'
            dialog.destroy()
        
        def on_already_active():
            user_choice['value'] = 'skip'
            dialog.destroy()
        
        def on_cancel():
            user_choice['value'] = 'cancel'
            dialog.destroy()
        
        ttk.Button(
            btn_frame,
            text="🌐 Open Azure Portal to Activate (Recommended)",
            command=on_open_portal,
            width=50
        ).pack(pady=5)
        
        ttk.Button(
            btn_frame,
            text="✓ Already activated - Continue",
            command=on_already_active,
            width=50
        ).pack(pady=5)
        
        ttk.Button(
            btn_frame,
            text="Cancel",
            command=on_cancel,
            width=50
        ).pack(pady=5)
        
        # Wait for dialog to close
        self.root.wait_window(dialog)
        
        # Handle user choice
        if user_choice['value'] == 'portal':
            # Open Azure Portal PIM page
            import webbrowser
            pim_url = "https://portal.azure.com/#view/Microsoft_Azure_PIMCommon/ActivationMenuBlade/~/aadmigratedroles"
            
            try:
                webbrowser.open(pim_url)
                self.log_message("Opened Azure Portal PIM activation page")
                
                messagebox.showinfo(
                    "Activate in Portal",
                    f"Azure Portal PIM page opened in your browser.\n\n"
                    f"Please activate these {num_eligible} role(s):\n\n" + 
                    "\n".join(f"  • {r}" for r in role_names[:10]) +
                    ("\n  ... and more" if len(role_names) > 10 else "") +
                    f"\n\nJustification: patching\n"
                    f"\nClick OK when you've activated the roles to continue."
                )
                
                self.log_message("User confirmed PIM roles activated in portal")
                self.refresh_inventory()
                
            except Exception as e:
                messagebox.showerror(
                    "Error",
                    f"Could not open browser: {e}\n\n"
                    f"Please manually navigate to:\n{pim_url}"
                )
            
        elif user_choice['value'] == 'skip':
            # User says they already activated
            self.log_message("User confirmed PIM roles already activated manually")
            messagebox.showinfo("Continuing", "Proceeding with manually activated PIM roles.")
            self.refresh_inventory()
            
        else:  # cancel
            # User cancelled
            self.log_message("User cancelled PIM activation")
            messagebox.showinfo(
                "Cancelled", 
                "Authentication successful, but PIM roles not activated.\n\n"
                "Please activate PIM roles and try again."
            )
    
    def on_auth_failure(self):
        """Handle authentication failure"""
        self.auth_status_label.config(text="✗ Authentication failed", foreground="red")
        messagebox.showerror(
            "Authentication Failed",
            "Please ensure you are logged in with Azure CLI:\n"
            "Run: az login"
        )
    
    def refresh_inventory(self):
        """Refresh VM inventory from Azure"""
        if not self.vm_inventory:
            messagebox.showwarning("Warning", "Please authenticate first")
            return
        
        tenant = self.current_tenant.get()
        selected_subs = self._get_selected_subscription_ids()
        if not selected_subs:
            messagebox.showwarning("Warning", "Please select a subscription option")
            return
        self.update_status("Fetching fresh VM inventory...")
        self.progress.start()
        
        def inventory_thread():
            try:
                # Get fresh VM list (single or all selected subscriptions)
                vms = []
                for sub_id in selected_subs:
                    self.auth_manager.set_subscription(sub_id)
                    sub_vms = self.vm_inventory.get_fresh_vm_list(tenant)
                    sub_vms = self.vm_inventory.refresh_vm_states(sub_vms)
                    for vm in sub_vms:
                        vm['subscription_id'] = sub_id
                    vms.extend(sub_vms)
                
                # Fetch last assessment times for each VM
                self.root.after(0, lambda: self.update_status("Fetching last patch assessment times..."))
                
                for idx, vm in enumerate(vms, 1):
                    if not vm.get('should_skip', False):
                        # Get last assessment timestamp
                        vm_sub = vm.get('subscription_id')
                        if vm_sub:
                            self.auth_manager.set_subscription(vm_sub)
                        last_checked = self.aum_manager.get_last_assessment_time(
                            vm['resource_group'],
                            vm['name']
                        )
                        vm['last_checked'] = last_checked if last_checked else '-'
                        
                        # Update progress
                        self.root.after(0, lambda i=idx, t=len(vms): 
                            self.update_status(f"Loading VMs ({i}/{t})...")
                        )
                    else:
                        vm['last_checked'] = '-'
                
                self.root.after(0, lambda: self.on_inventory_loaded(vms))
            except Exception as e:
                self.root.after(0, lambda: self.on_inventory_error(str(e)))
        
        threading.Thread(target=inventory_thread, daemon=True).start()
    
    def on_inventory_loaded(self, vms):
        """Handle loaded inventory"""
        self.vms = vms
        self.progress.stop()
        
        # Clear tree
        for item in self.vm_tree.get_children():
            self.vm_tree.delete(item)
        
        # Populate tree
        for vm in vms:
            status = "⚠️ Skip" if vm.get('should_skip') else "✓ Ready"
            last_checked = vm.get('last_checked', '-')
            
            self.vm_tree.insert('', tk.END, values=(
                vm['name'],
                vm['resource_group'],
                vm['os_type'],
                vm.get('power_state', 'unknown'),
                vm.get('type', 'Server'),
                status,
                last_checked
            ))
        
        # Save initial states
        self.power_manager.save_initial_states(self._get_scope_label().replace(" ", "_"), vms)
        
        self.update_status(f"Loaded {len(vms)} VMs")
        self.log_message(f"VM inventory refreshed: {len(vms)} VMs found")
    
    def on_inventory_error(self, error):
        """Handle inventory error"""
        self.progress.stop()
        messagebox.showerror("Error", f"Failed to load inventory: {error}")
    
    def start_deallocated_vms(self):
        """Start all deallocated VMs"""
        if not self.vms:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return
        
        response = messagebox.askyesno(
            "Confirm",
            "Start all deallocated VMs for patching?\n\n"
            "This may take several minutes."
        )
        
        if not response:
            return
        
        self.update_status("Starting deallocated VMs...")
        self.progress.start()
        
        def start_thread():
            dry_run = self.config.is_dry_run()
            results = self.power_manager.start_deallocated_vms(self.vms, dry_run=dry_run)
            
            self.root.after(0, lambda: self.on_vms_started(results))
        
        threading.Thread(target=start_thread, daemon=True).start()
    
    def on_vms_started(self, results):
        """Handle VMs started"""
        self.progress.stop()
        
        message = (
            f"Started: {len(results['started'])}\n"
            f"Already running: {len(results['already_running'])}\n"
            f"Skipped: {len(results['skipped'])}\n"
            f"Failed: {len(results['failed'])}"
        )
        
        self.log_message(f"VM start results: {message}")
        messagebox.showinfo("VMs Started", message)
        
        # Refresh inventory to update states
        self.refresh_inventory()
    
    def check_for_updates(self):
        """Check for updates on all VMs"""
        if not self.vms:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return
        
        if not self.aum_manager:
            messagebox.showwarning("Warning", "Please authenticate first")
            return
        
        selected_subs = self._get_selected_subscription_ids()
        if not selected_subs:
            messagebox.showwarning("Warning", "Please select a subscription option")
            return

        # Count VMs to assess
        vms_to_assess = [vm for vm in self.vms if not vm.get('should_skip', False)]
        
        if not vms_to_assess:
            messagebox.showinfo("Info", "No VMs to assess (all marked to skip)")
            return
        
        response = messagebox.askyesno(
            "Check for Updates",
            f"Assess patches on {len(vms_to_assess)} VMs?\n\n"
            f"This will trigger 'Check for Updates' in Azure Update Manager.\n"
            f"This may take 10-15 minutes depending on number of VMs.\n\n"
            f"Progress will be shown in the Monitoring tab."
        )
        
        if not response:
            return
        
        self.cancel_update_check = False
        self.is_checking_updates = True
        self.update_status(f"Checking for updates on {len(vms_to_assess)} VMs...")
        self.log_message(f"Starting update assessment for {len(vms_to_assess)} VMs")
        self.progress.start()
        
        # Switch to monitoring tab
        # notebook.select(2)  # Would need reference to notebook
        
        def assess_thread():
            try:
                total = len(vms_to_assess)
                completed = [0]
                cancel_flag = [False]
                results = {}

                def _progress(vm_name, result, done, _total):
                    completed[0] = done
                    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    # Find vm dict to update tree
                    vm_match = next((v for v in vms_to_assess if v['name'] == vm_name), None)
                    if vm_match:
                        vm_match['last_checked'] = ts

                    if result.get('status') == 'failed':
                        self.root.after(0, lambda n=vm_name, i=done, t=total:
                            self.log_message(f"[{i}/{t}] ✗ {n}: Assessment failed", level='WARNING')
                        )
                    else:
                        patch_count = result.get('available_patch_count', 0)
                        self.root.after(0, lambda n=vm_name, c=patch_count, i=done, t=total:
                            self.log_message(f"[{i}/{t}] ✓ {n}: {c} patches available")
                        )

                    if vm_match:
                        self.root.after(0, lambda v=vm_match: self.update_vm_in_tree(v))
                    self.root.after(0, lambda i=done, t=total:
                        self.update_status(f"Assessing... {i}/{t} complete (concurrent)")
                    )

                self.root.after(0, lambda t=total:
                    self.log_message(f"Starting concurrent assessment of {t} VMs (all at once)")
                )

                results = self.aum_manager.assess_patches_concurrent(
                    vms_to_assess,
                    max_workers=20,
                    progress_callback=_progress,
                    cancel_flag=cancel_flag,
                )

                # Wire cancel flag to UI cancel
                def _check_cancel():
                    if self.cancel_update_check:
                        cancel_flag[0] = True

                self.root.after(0, _check_cancel)
                self.root.after(0, lambda r=results: self.on_assessment_complete(r))

            except Exception as e:
                self.root.after(0, lambda err=str(e): self.on_assessment_error(err))
        
        threading.Thread(target=assess_thread, daemon=True).start()

    def stop_check_for_updates(self):
        """Signal running update assessment to stop after current VM completes"""
        if not self.is_checking_updates:
            messagebox.showinfo("Info", "No update check currently running")
            return
        self.cancel_update_check = True
        self.log_message("Stop requested: assessment will halt after current VM.", level='WARNING')
        self.update_status("Stopping update assessment...")
    
    def on_assessment_complete(self, results):
        """Handle completed assessment"""
        self.progress.stop()
        self.is_checking_updates = False
        
        success_count = sum(1 for r in results.values() if r.get('status') != 'failed')
        failed_count = sum(1 for r in results.values() if r.get('status') == 'failed')
        total_patches = sum(r.get('available_patch_count', 0) for r in results.values() if r.get('status') != 'failed')
        
        summary = (
            f"Assessment Complete!\n\n"
            f"VMs assessed: {success_count}\n"
            f"Failed: {failed_count}\n"
            f"Total patches available: {total_patches}\n\n"
            f"You can now proceed to execute patches."
        )
        
        self.log_message(f"Assessment completed: {success_count} VMs, {total_patches} patches available")
        self.update_status(f"Assessment complete: {total_patches} patches available across {success_count} VMs")
        
        messagebox.showinfo("Assessment Complete", summary)
    
    def update_vm_in_tree(self, vm):
        """Update a single VM's row in the tree view"""
        # Find the VM in the tree
        for item in self.vm_tree.get_children():
            values = self.vm_tree.item(item)['values']
            if values[0] == vm['name']:  # Match by VM name
                status = "⚠️ Skip" if vm.get('should_skip') else "✓ Ready"
                last_checked = vm.get('last_checked', '-')
                
                # Update the row
                self.vm_tree.item(item, values=(
                    vm['name'],
                    vm['resource_group'],
                    vm['os_type'],
                    vm.get('power_state', 'unknown'),
                    vm.get('type', 'Server'),
                    status,
                    last_checked
                ))
                break
    
    def on_assessment_error(self, error):
        """Handle assessment error"""
        self.progress.stop()
        self.is_checking_updates = False
        self.log_message(f"Assessment error: {error}", level='ERROR')
        self.update_status("Assessment failed - check logs")
        messagebox.showerror("Assessment Failed", f"Error during assessment:\n{error}")
    
    def show_unsupported_menu(self):
        """Show menu for patching unsupported VMs"""
        if not self.vms:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return
        
        # Create popup window
        menu_window = tk.Toplevel(self.root)
        menu_window.title("Patch Unsupported VMs")
        menu_window.geometry("700x500")
        menu_window.configure(bg=self.colors['bg_dark'])
        
        # Title
        title_label = ttk.Label(
            menu_window,
            text="Patch Unsupported VMs via Run Command",
            font=('Arial', 14, 'bold')
        )
        title_label.pack(pady=10)
        
        # Info
        info_text = (
            "These VMs will be patched using direct commands:\n"
            "• Linux: sudo apt update && sudo apt upgrade -y\n"
            "• Windows: PSWindowsUpdate module (Check for updates)\n\n"
            "⚠️ You will be prompted for credentials for each VM"
        )
        info_label = ttk.Label(menu_window, text=info_text, justify=tk.LEFT)
        info_label.pack(pady=5, padx=10)
        
        # List of VMs
        ttk.Label(menu_window, text="Select VMs to patch:", font=('Arial', 10, 'bold')).pack(pady=5)
        
        # Treeview for VM selection
        columns = ('Select', 'VM Name', 'OS Type', 'Power State')
        vm_list = ttk.Treeview(menu_window, columns=columns, show='headings', height=12)
        
        vm_list.heading('Select', text='✓')
        vm_list.heading('VM Name', text='VM Name')
        vm_list.heading('OS Type', text='OS Type')
        vm_list.heading('Power State', text='Power State')
        
        vm_list.column('Select', width=40)
        vm_list.column('VM Name', width=200)
        vm_list.column('OS Type', width=100)
        vm_list.column('Power State', width=100)
        
        # Track selections
        selected_vms = set()
        
        def toggle_selection(event):
            item = vm_list.selection()[0]
            values = vm_list.item(item)['values']
            vm_name = values[1]
            
            if vm_name in selected_vms:
                selected_vms.remove(vm_name)
                vm_list.item(item, values=('', values[1], values[2], values[3]))
            else:
                selected_vms.add(vm_name)
                vm_list.item(item, values=('✓', values[1], values[2], values[3]))
        
        vm_list.bind('<Double-Button-1>', toggle_selection)
        
        # Populate VMs
        for vm in self.vms:
            vm_list.insert('', tk.END, values=(
                '',
                vm['name'],
                vm['os_type'],
                vm.get('power_state', 'unknown')
            ))
        
        vm_list.pack(pady=5, padx=10, fill=tk.BOTH, expand=True)
        
        # Buttons
        button_frame = ttk.Frame(menu_window)
        button_frame.pack(pady=10)
        
        def patch_selected():
            if not selected_vms:
                messagebox.showwarning("Warning", "Please select at least one VM")
                return
            
            vms_to_patch = [vm for vm in self.vms if vm['name'] in selected_vms]
            menu_window.destroy()
            self.execute_patches_run_command(vms_to_patch)
        
        def patch_all():
            if not messagebox.askyesno("Confirm", f"Patch all {len(self.vms)} VMs via Run Command?"):
                return
            menu_window.destroy()
            self.execute_patches_run_command(self.vms)
        
        ttk.Button(button_frame, text="Patch Selected VMs", command=patch_selected, width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Patch All VMs", command=patch_all, width=20).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=menu_window.destroy, width=15).pack(side=tk.LEFT, padx=5)
    
    def execute_patches_run_command(self, vms_to_patch=None):
        """Execute patches on unsupported VMs using Run Command API"""
        if vms_to_patch is None:
            vms_to_patch = self.vms
        
        if not vms_to_patch:
            messagebox.showwarning("Warning", "No VMs to patch")
            return
        
        # Confirm
        response = messagebox.askyesno(
            "Confirm Patching",
            f"Patch {len(vms_to_patch)} VMs using Azure Run Command?\n\n"
            "This will execute:\n"
            "• Linux: sudo apt update && sudo apt upgrade -y\n"
            "• Windows: PSWindowsUpdate (Check and install updates)\n\n"
            "This may take 30+ minutes per VM."
        )
        
        if not response:
            return
        
        self.update_status(f"Patching {len(vms_to_patch)} VMs via Run Command...")
        self.log_message(f"Starting Run Command patching for {len(vms_to_patch)} VMs")
        self.progress.start()
        
        def patch_thread():
            try:
                total = len(vms_to_patch)
                agg = {'success': [], 'failed': [], 'skipped': []}
                run_id = new_run_id()
                tenant = self.current_tenant.get()

                def _progress(vm_name, result, done, _total):
                    status = result.get('status', 'unknown')
                    attempt = result.get('attempt', 1)
                    if status == 'success':
                        agg['success'].append(vm_name)
                        self.root.after(0, lambda n=vm_name, a=attempt, i=done, t=total:
                            self.log_message(f"[{i}/{t}] ✓ {n}: patched (attempt {a})")
                        )
                    elif status == 'skipped':
                        agg['skipped'].append(vm_name)
                        reason = result.get('reason', '')
                        self.root.after(0, lambda n=vm_name, r=reason, i=done, t=total:
                            self.log_message(f"[{i}/{t}] ⊘ {n}: skipped — {r}", level='INFO')
                        )
                    else:
                        agg['failed'].append(vm_name)
                        err = result.get('error', result.get('reason', 'unknown error'))
                        self.root.after(0, lambda n=vm_name, e=err, a=attempt, i=done, t=total:
                            self.log_message(f"[{i}/{t}] ✗ {n}: failed after {a} attempt(s) — {e}", level='ERROR')
                        )

                    # Record final outcome to local DB (retry counts shown in GUI log, not stored)
                    if self.db:
                        vm_match = next((v for v in vms_to_patch if v['name'] == vm_name), None)
                        try:
                            self.db.record_patch_run(
                                run_id=run_id, tenant=tenant, vm_name=vm_name,
                                resource_group=vm_match['resource_group'] if vm_match else '',
                                subscription_id=vm_match.get('subscription_id') if vm_match else None,
                                patches_applied=result.get('patches_installed', 0),
                                status=status,
                                error_detail=result.get('error'),
                            )
                        except Exception as db_err:
                            logger.debug(f"DB record failed (non-fatal): {db_err}")

                    self.root.after(0, lambda i=done, t=total:
                        self.update_status(f"Patching... {i}/{t} complete (concurrent, up to 2 retries)")
                    )

                self.root.after(0, lambda t=total:
                    self.log_message(f"Starting concurrent Run Command patching of {t} VMs (up to 2 retries each)")
                )

                self.patch_executor.execute_patches_concurrent(
                    vms_to_patch,
                    dry_run=False,
                    max_workers=15,
                    max_retries=2,
                    progress_callback=_progress,
                    cancel_flag=self._cancel_all_flag,
                )

                self.root.after(0, lambda r=agg: self.on_run_command_complete(r))

            except Exception as e:
                self.root.after(0, lambda err=str(e): self.on_run_command_error(err))
        
        threading.Thread(target=patch_thread, daemon=True).start()
    
    def on_run_command_complete(self, results):
        """Handle completed Run Command patching — shows retry stats in results dialog."""
        self.progress.stop()

        success = results.get('success', [])
        failed  = results.get('failed', [])
        skipped = results.get('skipped', [])
        total   = len(success) + len(failed) + len(skipped)

        # Count VMs that required retries (logged per-VM in the log tab with attempt numbers)
        # Scan the log for "[attempt 2]" or "[attempt 3]" lines to surface the count here
        retry_note = ("See the Logs tab for per-VM retry details (each attempt is logged "
                      "as it happens with attempt number).")

        summary = (
            f"Run Command Patching Complete\n"
            f"{'─' * 38}\n"
            f"  Total processed : {total}\n"
            f"  ✓ Succeeded     : {len(success)}\n"
            f"  ✗ Failed        : {len(failed)}\n"
            f"  ⊘ Skipped       : {len(skipped)}\n\n"
            f"Retry behaviour: up to 2 auto-retries per VM.\n"
            f"{retry_note}\n"
        )

        if failed:
            summary += f"\nFailed VMs:\n"
            summary += "\n".join(f"  • {vm}" for vm in failed[:12])
            if len(failed) > 12:
                summary += f"\n  ... and {len(failed) - 12} more"

        self.log_message(
            f"Patching complete — {len(success)} succeeded, {len(failed)} failed, {len(skipped)} skipped"
        )
        self.update_status(f"Done: {len(success)}/{total} patched, {len(failed)} failed")
        messagebox.showinfo("Patching Complete", summary)
    
    def on_run_command_error(self, error):
        """Handle Run Command error"""
        self.progress.stop()
        self.log_message(f"Run Command patching error: {error}", level='ERROR')
        messagebox.showerror("Error", f"Run Command patching failed:\n{error}")
    
    def execute_patches_aum(self):
        """Execute patches via AUM"""
        self.log_message("Patch execution via AUM would start here")
        messagebox.showinfo("Info", "Patch execution will be implemented in integration phase")
    
    def update_excel(self):
        """Update Excel tracking"""
        self.log_message("Updating Excel file...")
        messagebox.showinfo("Info", "Excel update will be implemented in integration phase")
    
    def generate_summary(self):
        """Generate completion summary"""
        tenant = self.current_tenant.get()
        summary = self.excel_tracker.create_summary_report(tenant)
        
        message = (
            f"Tenant: {summary['tenant']}\n"
            f"Cycle: {summary['cycle']}\n"
            f"Total VMs: {summary['total_vms']}\n"
            f"Success: {summary['success']}\n"
            f"Failed: {summary['failed']}\n"
            f"Skipped: {summary['skipped']}\n"
            f"Manual Review: {summary['manual_review']}"
        )
        
        self.log_message(f"Summary: {message}")
        messagebox.showinfo("Summary", message)
    
    def stop_started_vms(self):
        """Stop all VMs that were started for patching (wrapper for restore_vm_states)"""
        if not self.power_manager:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return
        
        response = messagebox.askyesno(
            "STOP Started VMs",
            "Stop ONLY VMs that were OFF before patching and were turned ON by this tool?\n\n"
            "This will:\n"
            "• Stop/deallocate VMs that were initially deallocated or stopped\n"
            "• NEVER stop VMs that were already running before patching\n\n"
            "Safe to run."
        )
        
        if not response:
            return
        
        self.restore_vm_states()
    
    def restore_vm_states(self):
        """Restore VMs to initial power states"""
        if not self.power_manager:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return
        
        self.update_status("Stopping VMs that were started for patching...")
        self.progress.start()
        
        def restore_thread():
            dry_run = self.config.is_dry_run()
            results = self.power_manager.restore_initial_states(dry_run=dry_run)
            
            self.root.after(0, lambda: self.on_states_restored(results))
        
        threading.Thread(target=restore_thread, daemon=True).start()
    
    def on_states_restored(self, results):
        """Handle states restored"""
        self.progress.stop()
        
        message = (
            f"Stopped: {len(results['stopped'])}\n"
            f"Left running: {len(results['left_running'])}\n"
            f"Failed: {len(results['failed'])}"
        )
        
        self.log_message(f"VM state restoration: {message}")
        messagebox.showinfo("States Restored", message)
    
    def export_asset_inventory(self):
        """Export Complete Asset Details from Azure Resource Graph"""
        if not self.auth_manager.credential:
            messagebox.showwarning("Not Authenticated", "Please authenticate first")
            return
        
        self.update_status("Exporting Complete Asset Details...")
        self.progress.start()
        
        def export_thread():
            try:
                csv_path = self.resource_graph_exporter.export_complete_asset_details()
                self.root.after(0, lambda: self.on_asset_export_complete(csv_path))
            except Exception as e:
                error_msg = f"Error exporting asset inventory: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.root.after(0, lambda: self.on_export_error(error_msg))
        
        threading.Thread(target=export_thread, daemon=True).start()
    
    def export_pre_patch_data(self):
        """Export Pre-Patch Data from Azure Resource Graph"""
        if not self.auth_manager.credential:
            messagebox.showwarning("Not Authenticated", "Please authenticate first")
            return
        
        self.update_status("Exporting Pre-Patch Data...")
        self.progress.start()
        
        def export_thread():
            try:
                csv_path = self.resource_graph_exporter.export_pre_patch_data()
                self.root.after(0, lambda: self.on_prepatch_export_complete(csv_path))
            except Exception as e:
                error_msg = f"Error exporting pre-patch data: {str(e)}"
                logger.error(error_msg, exc_info=True)
                self.root.after(0, lambda: self.on_export_error(error_msg))
        
        threading.Thread(target=export_thread, daemon=True).start()
    
    def on_asset_export_complete(self, csv_path):
        """Handle completion of asset inventory export"""
        self.progress.stop()
        
        if csv_path:
            self.update_status("Asset inventory exported successfully")
            self.log_message(f"Asset inventory exported to: {csv_path}")
            messagebox.showinfo(
                "Export Complete",
                f"Complete Asset Details exported to:\n{csv_path}"
            )
        else:
            self.update_status("Asset inventory export failed")
            messagebox.showerror(
                "Export Failed",
                "Failed to export Complete Asset Details. Check logs for details."
            )
    
    def on_prepatch_export_complete(self, csv_path):
        """Handle completion of pre-patch data export"""
        self.progress.stop()
        
        if csv_path:
            self.update_status("Pre-patch data exported successfully")
            self.log_message(f"Pre-patch data exported to: {csv_path}")
            messagebox.showinfo(
                "Export Complete",
                f"Pre-Patch Export completed:\n{csv_path}"
            )
        else:
            self.update_status("Pre-patch data export failed")
            messagebox.showerror(
                "Export Failed",
                "Failed to export Pre-Patch Data. Check logs for details."
            )
    
    def on_export_error(self, error_msg):
        """Handle export errors"""
        self.progress.stop()
        self.update_status("Export failed")
        self.log_message(f"Export error: {error_msg}")
        messagebox.showerror("Export Error", error_msg)

    # ------------------------------------------------------------------ #
    # In-tool Azure Login                                                  #
    # ------------------------------------------------------------------ #

    def show_login_dialog(self):
        """
        Show an in-tool device-code login dialog.
        Operators can authenticate without leaving the app or pre-running az login.
        """
        dialog = tk.Toplevel(self.root)
        dialog.title("Login to Azure")
        dialog.geometry("680x460")
        dialog.transient(self.root)
        dialog.grab_set()
        dialog.configure(bg=self.colors['bg_dark'])

        x = self.root.winfo_x() + (self.root.winfo_width() - 680) // 2
        y = self.root.winfo_y() + (self.root.winfo_height() - 460) // 2
        dialog.geometry(f"+{x}+{y}")

        ttk.Label(dialog, text="Azure Device-Code Login",
                  font=('Arial', 13, 'bold')).pack(pady=(18, 4))
        ttk.Label(dialog,
                  text="Click 'Start Login' — a URL and code will appear below.\n"
                       "Open the URL in your browser, enter the code, then sign in.",
                  justify='center').pack(pady=(0, 10))

        output_box = scrolledtext.ScrolledText(dialog, height=10, width=72,
                                               bg=self.colors['bg_light'],
                                               fg=self.colors['text'],
                                               font=('Courier', 9))
        output_box.pack(padx=16, pady=4)
        output_box.insert(tk.END, "(login output will appear here)\n")
        output_box.config(state='disabled')

        btn_frame = ttk.Frame(dialog)
        btn_frame.pack(pady=10)

        login_proc = [None]

        def _append(line: str):
            output_box.config(state='normal')
            output_box.insert(tk.END, line + "\n")
            output_box.see(tk.END)
            output_box.config(state='disabled')

            # If the URL/code line appears, also open browser automatically
            if "microsoft.com/devicelogin" in line.lower():
                import re
                urls = re.findall(r'https?://\S+', line)
                if urls:
                    webbrowser.open(urls[0])

        def _start_login():
            start_btn.config(state='disabled')
            output_box.config(state='normal')
            output_box.delete('1.0', tk.END)
            output_box.config(state='disabled')

            def _on_line(line):
                dialog.after(0, lambda l=line: _append(l))

            def _on_done(success):
                msg = "Login succeeded." if success else "Login failed — check output above."
                dialog.after(0, lambda: _append(f"\n>>> {msg}"))
                dialog.after(0, lambda: start_btn.config(state='normal'))
                if success:
                    dialog.after(0, lambda: self.log_message("In-tool Azure login succeeded"))

            login_proc[0] = self.auth_manager.login_with_device_code(
                output_callback=_on_line, on_complete=_on_done
            )

        start_btn = ttk.Button(btn_frame, text="Start Login", command=_start_login)
        start_btn.pack(side=tk.LEFT, padx=8)
        ttk.Button(btn_frame, text="Close", command=dialog.destroy).pack(side=tk.LEFT, padx=8)

    # ------------------------------------------------------------------ #
    # Pre-patch Health Gate                                                #
    # ------------------------------------------------------------------ #

    def run_health_gate(self):
        """Run pre-patch health checks on all running VMs concurrently."""
        if not self.health_gate:
            messagebox.showwarning("Warning", "Please authenticate first")
            return
        if not self.vms:
            messagebox.showwarning("Warning", "Please refresh inventory first")
            return

        running_vms = [v for v in self.vms if v.get('power_state') == 'running' and not v.get('should_skip')]
        if not running_vms:
            messagebox.showinfo("Health Gate", "No running VMs to check.")
            return

        response = messagebox.askyesno(
            "Run Health Gate",
            f"Check {len(running_vms)} running VMs for:\n"
            "  • VM agent responsiveness\n"
            "  • Disk space (< 10% free = flag)\n"
            "  • Pending reboot (Windows)\n\n"
            "VMs that fail will be marked to skip during patching.\n"
            "Continue?"
        )
        if not response:
            return

        self.update_status(f"Running health gate on {len(running_vms)} VMs (concurrent)...")
        self.progress.start()

        def _gate_thread():
            try:
                gate_results = {}
                completed = [0]
                total = len(running_vms)

                def _progress(vm_name, result, done, _total):
                    completed[0] = done
                    icon = "✓" if result.passed else "✗"
                    self.root.after(0, lambda n=vm_name, r=result, i=done, t=_total:
                        self.log_message(
                            f"[{i}/{t}] {icon} Health gate {n}: {r.summary()}"
                        )
                    )
                    self.root.after(0, lambda i=done, t=_total:
                        self.update_status(f"Health gate: {i}/{t} checked")
                    )

                gate_results = self.health_gate.check_batch(
                    self.vms,
                    max_workers=15,
                    progress_callback=_progress,
                )

                # Mark VMs that failed the gate
                failed_names = []
                for vm in self.vms:
                    r = gate_results.get(vm['name'])
                    if r and not r.passed:
                        vm['health_gate_failed'] = True
                        vm['skip_reason'] = (vm.get('skip_reason') or '') + f" [health gate: {r.summary()}]"
                        failed_names.append(vm['name'])
                    elif r and r.passed:
                        vm['health_gate_failed'] = False

                self.root.after(0, lambda f=failed_names, t=total: self._on_health_gate_done(f, t))

            except Exception as exc:
                self.root.after(0, lambda e=str(exc): (
                    self.progress.stop(),
                    self.log_message(f"Health gate error: {e}", level='ERROR'),
                    messagebox.showerror("Health Gate Error", e)
                ))

        threading.Thread(target=_gate_thread, daemon=True).start()

    def _on_health_gate_done(self, failed_names: list, total: int):
        self.progress.stop()
        passed = total - len(failed_names)
        msg = f"Health gate complete.\n\n✓ {passed}/{total} VMs passed."
        if failed_names:
            msg += f"\n\n✗ {len(failed_names)} VMs flagged and will be skipped:\n"
            msg += "\n".join(f"  • {n}" for n in failed_names[:15])
            if len(failed_names) > 15:
                msg += f"\n  ... and {len(failed_names) - 15} more"
        self.update_status(f"Health gate: {passed}/{total} passed")
        messagebox.showinfo("Health Gate Results", msg)

    # ------------------------------------------------------------------ #
    # All-Tenant Dashboard Tab                                             #
    # ------------------------------------------------------------------ #

    def setup_dashboard_tab(self, notebook):
        """Dashboard showing all tenants simultaneously with live status."""
        dash_frame = ttk.Frame(notebook, padding="10")
        notebook.add(dash_frame, text="All Tenants")

        # Header
        ttk.Label(dash_frame, text="Multi-Tenant Patch Dashboard",
                  font=('Arial', 12, 'bold')).grid(row=0, column=0, columnspan=4, pady=(0, 10))

        # Per-tenant status rows
        tenants = self.config.get_tenant_list() if hasattr(self.config, 'get_tenant_list') else []

        for col, header in enumerate(["Tenant", "Status", "VMs", "Progress"]):
            ttk.Label(dash_frame, text=header, font=('Arial', 10, 'bold'),
                      foreground=self.colors['blue_light']).grid(row=1, column=col, padx=10, pady=4, sticky='w')

        for row_idx, tenant in enumerate(tenants, 2):
            status_var  = tk.StringVar(value="Idle")
            vm_count_var = tk.StringVar(value="—")
            progress_var = tk.StringVar(value="—")

            ttk.Label(dash_frame, text=tenant, font=('Arial', 10)).grid(
                row=row_idx, column=0, padx=10, sticky='w')
            status_lbl = ttk.Label(dash_frame, textvariable=status_var, width=28)
            status_lbl.grid(row=row_idx, column=1, padx=10, sticky='w')
            ttk.Label(dash_frame, textvariable=vm_count_var, width=10).grid(
                row=row_idx, column=2, padx=10, sticky='w')
            ttk.Label(dash_frame, textvariable=progress_var, width=22).grid(
                row=row_idx, column=3, padx=10, sticky='w')

            self._dashboard_widgets[tenant] = {
                'status': status_var,
                'vm_count': vm_count_var,
                'progress': progress_var,
                'label': status_lbl,
            }

        # Control buttons
        btn_row = len(tenants) + 3
        btn_frame = ttk.Frame(dash_frame)
        btn_frame.grid(row=btn_row, column=0, columnspan=4, pady=20, sticky='w')

        ttk.Button(btn_frame, text="Authenticate All Tenants",
                   command=self.authenticate_all_tenants).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Refresh All Inventories",
                   command=self.refresh_all_inventories).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="Run Health Gate — All Tenants",
                   command=self.health_gate_all_tenants).pack(side=tk.LEFT, padx=6)
        ttk.Button(btn_frame, text="STOP All Operations",
                   command=self._stop_all_operations).pack(side=tk.LEFT, padx=6)

        # DB info
        db_row = btn_row + 1
        if self.db:
            ttk.Label(dash_frame,
                      text=f"Local DB: {self.db.get_db_path()}",
                      foreground=self.colors['text_dim'],
                      font=('Arial', 8)).grid(row=db_row, column=0, columnspan=4, pady=(10, 0), sticky='w')

        dash_frame.columnconfigure(1, weight=1)

    def _update_dashboard(self, tenant: str, status: str, vm_count: str = None, progress: str = None,
                          color: str = None):
        """Thread-safe dashboard update — call via root.after()."""
        def _do():
            w = self._dashboard_widgets.get(tenant)
            if not w:
                return
            w['status'].set(status)
            if vm_count is not None:
                w['vm_count'].set(vm_count)
            if progress is not None:
                w['progress'].set(progress)
            if color and 'label' in w:
                w['label'].config(foreground=color)
        self.root.after(0, _do)

    # ------------------------------------------------------------------ #
    # Multi-tenant concurrent operations                                   #
    # ------------------------------------------------------------------ #

    def authenticate_all_tenants(self):
        """Authenticate to all configured tenants concurrently."""
        tenants = self.config.get_tenant_list() if hasattr(self.config, 'get_tenant_list') else []
        if not tenants:
            messagebox.showwarning("Warning", "No tenants configured")
            return

        if not self.auth_manager.verify_cli_login():
            if messagebox.askyesno("Not Logged In",
                                   "You are not logged into Azure CLI.\nOpen the login dialog?"):
                self.show_login_dialog()
            return

        self.update_status("Authenticating all tenants concurrently...")
        self.progress.start()

        def _auth_all():
            results = {}
            for tenant in tenants:
                self._update_dashboard(tenant, "Authenticating...", color=self.colors['blue_light'])
            with ThreadPoolExecutor(max_workers=len(tenants)) as pool:
                future_map = {pool.submit(self.auth_manager.authenticate_for_tenant, t): t
                              for t in tenants}
                for future in as_completed(future_map):
                    tenant = future_map[future]
                    try:
                        ok = future.result()
                    except Exception:
                        ok = False
                    results[tenant] = ok
                    if ok:
                        self._update_dashboard(tenant, "Authenticated", color="green")
                    else:
                        self._update_dashboard(tenant, "Auth failed", color="red")

            ok_count = sum(1 for v in results.values() if v)
            self.root.after(0, lambda: (
                self.progress.stop(),
                self.update_status(f"Authenticated {ok_count}/{len(tenants)} tenants"),
            ))

        threading.Thread(target=_auth_all, daemon=True).start()

    def refresh_all_inventories(self):
        """Refresh inventory for all authenticated tenants concurrently."""
        tenants = self.config.get_tenant_list() if hasattr(self.config, 'get_tenant_list') else []
        authenticated = [t for t in tenants if self.auth_manager.is_tenant_authenticated(t)]

        if not authenticated:
            messagebox.showwarning("Warning", "No tenants authenticated. Run 'Authenticate All Tenants' first.")
            return

        self.progress.start()
        self.update_status("Refreshing inventories for all tenants concurrently...")

        # Temp VMInventory per tenant (shared auth_manager, separate client per call)
        inv = VMInventory(self.config, self.auth_manager, self.error_handler, db=self.db)

        def _refresh_one(tenant):
            self._update_dashboard(tenant, "Fetching inventory...", color=self.colors['blue_light'])
            try:
                sub_ids = self.config.get_subscription_ids(tenant)
                all_vms = []
                for sub_id in (sub_ids or [None]):
                    self.auth_manager.set_subscription(sub_id) if sub_id else None
                    vms = inv.get_fresh_vm_list(tenant, fetch_ips=True, sync_db=True,
                                                subscription_id=sub_id)
                    all_vms.extend(vms)
                db_count = self.db.get_vm_count(tenant) if self.db else len(all_vms)
                self._update_dashboard(tenant, "Inventory ready",
                                       vm_count=str(len(all_vms)),
                                       progress=f"{db_count} in DB",
                                       color="green")
                self.root.after(0, lambda t=tenant, n=len(all_vms):
                    self.log_message(f"[{t}] Inventory: {n} VMs fetched + synced to local DB")
                )
                return tenant, all_vms
            except Exception as exc:
                self._update_dashboard(tenant, f"Error: {str(exc)[:30]}", color="red")
                self.root.after(0, lambda t=tenant, e=str(exc):
                    self.log_message(f"[{t}] Inventory error: {e}", level='ERROR')
                )
                return tenant, []

        def _run():
            with ThreadPoolExecutor(max_workers=len(authenticated)) as pool:
                futures = {pool.submit(_refresh_one, t): t for t in authenticated}
                for f in as_completed(futures):
                    f.result()  # errors logged inside

            self.root.after(0, lambda: (
                self.progress.stop(),
                self.update_status("All inventories refreshed"),
            ))

        threading.Thread(target=_run, daemon=True).start()

    def health_gate_all_tenants(self):
        """Run health gate for all tenants that have inventory loaded in DB."""
        messagebox.showinfo(
            "Health Gate — All Tenants",
            "This will run health checks on VMs in DB.\n\n"
            "Use 'Refresh All Inventories' first, then switch to each tenant tab "
            "and use 'Run Health Gate' for targeted per-tenant checking with live tree updates.",
        )

    def _stop_all_operations(self):
        """Signal all concurrent operations to abort."""
        self._cancel_all_flag[0] = True
        self.cancel_update_check = True
        self.log_message("STOP requested — all pending operations will abort after current VM", level='WARNING')
        self.update_status("Stopping all operations...")
        # Reset flag after short delay so future runs aren't blocked
        self.root.after(5000, lambda: self._cancel_all_flag.__setitem__(0, False))


def main():
    """Main entry point"""
    root = tk.Tk()
    app = AUMAutomationGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
