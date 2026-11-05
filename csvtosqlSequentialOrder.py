import os
import time
import shutil
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import pyodbc
import re
import json
from datetime import datetime
from dateutil import parser
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from flask import Flask, render_template_string, jsonify, request, redirect, url_for, session
from werkzeug.security import generate_password_hash, check_password_hash
from functools import wraps
import secrets
from threading import Thread, Lock, Event, RLock
from collections import deque
from queue import Queue, Empty
import chardet
from concurrent.futures import ThreadPoolExecutor
import traceback
import signal
import sys
from contextlib import contextmanager

# ========================= GRACEFUL SHUTDOWN HANDLER =========================
class GracefulKiller:
    """Handle shutdown signals properly"""
    kill_now = False
    
    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
    
    def exit_gracefully(self, *args):
        logging.info("🛑 Shutdown signal received...")
        self.kill_now = True
        shutdown_event.set()


# ========================= CONFIGURATION FILE =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(BASE_DIR, "models_config.json")

# ========================= STATS PERSISTENCE =========================
STATS_FILE = os.path.join(BASE_DIR, "stats_history.json")

def load_stats_from_file():
    """Load stats from file on startup"""
    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, 'r') as f:
                saved_stats = json.load(f)
                logging.info(f"✅ Loaded stats history from {STATS_FILE}")
                return saved_stats
        except Exception as e:
            logging.error(f"Error loading stats: {e}")
    return {}

def save_stats_to_file(stats_dict):
    """Save stats to file periodically"""
    try:
        with open(STATS_FILE, 'w') as f:
            json.dump(stats_dict, f, indent=4)
    except Exception as e:
        logging.error(f"Error saving stats: {e}")


# Default configuration template with sequence numbers
DEFAULT_CONFIG = {}

def migrate_old_config(config):
    """Migrate old config with db_conn_str to new separate fields"""
    migrated = False
    for model_name, model_config in config.items():
        if 'db_conn_str' in model_config and 'db_driver' not in model_config:
            conn_str = model_config['db_conn_str']
            
            driver_match = re.search(r'DRIVER={([^}]+)}', conn_str)
            server_match = re.search(r'SERVER=([^;]+)', conn_str)
            database_match = re.search(r'DATABASE=([^;]+)', conn_str)
            uid_match = re.search(r'UID=([^;]+)', conn_str)
            pwd_match = re.search(r'PWD=([^;]+)', conn_str)
            
            model_config['db_driver'] = driver_match.group(1) if driver_match else "ODBC Driver 17 for SQL Server"
            model_config['db_server'] = server_match.group(1) if server_match else "localhost"
            model_config['db_name'] = database_match.group(1) if database_match else "Production_Data"
            model_config['db_username'] = uid_match.group(1) if uid_match else "sa"
            model_config['db_password'] = pwd_match.group(1) if pwd_match else ""
            
            del model_config['db_conn_str']
            migrated = True
            logging.info(f"✅ Migrated configuration for model: {model_name}")
    
    # Add sequence numbers if missing
    for i, model_name in enumerate(config.keys(), 1):
        if 'sequence_number' not in config[model_name]:
            config[model_name]['sequence_number'] = i
            migrated = True
            logging.info(f"✅ Added sequence number {i} for model: {model_name}")
    
    return migrated

def load_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            if migrate_old_config(config):
                save_config(config)
                logging.info("✅ Configuration migrated to new format")
            return config
    else:
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG

def save_config(config):
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config, f, indent=4)

UNIFIED_CONFIGS = load_config()

LOG_BACKUP_DAYS = 10
WEB_PORT = 4200
WEB_HOST = "0.0.0.0"

# ========================= STATS CLASSES =========================
class LockFreeStats:
    """Thread-safe stats without blocking locks"""
    def __init__(self):
        self._stats = {}
        self._lock = RLock()
        self._last_save_time = time.time()
        self._save_interval = 30  # Save every 30 seconds
        
    def initialize(self, model_name):
        """Initialize stats for a model"""
        try:
            with self._lock:
                # Load from saved stats if exists
                saved_stats = load_stats_from_file()
                
                if model_name in saved_stats:
                    # Restore saved stats
                    self._stats[model_name] = {
                        "total_files": saved_stats[model_name].get("total_files", 0),
                        "total_rows": saved_stats[model_name].get("total_rows", 0),
                        "failed_files": saved_stats[model_name].get("failed_files", 0),
                        "avg_process_time": saved_stats[model_name].get("avg_process_time", 0),
                        "last_file": saved_stats[model_name].get("last_file"),
                        "last_processed": saved_stats[model_name].get("last_processed"),
                        "status": "Idle",
                        "type": saved_stats[model_name].get("type", "unknown"),
                        "sequence_number": saved_stats[model_name].get("sequence_number", 999),
                        "_process_times": deque(maxlen=50)
                    }
                    logging.info(f"[{model_name}] ✅ Restored stats: {self._stats[model_name]['total_files']} files, {self._stats[model_name]['failed_files']} failed")
                else:
                    # Create new stats
                    self._stats[model_name] = {
                        "total_files": 0,
                        "total_rows": 0,
                        "failed_files": 0,
                        "avg_process_time": 0,
                        "last_file": None,
                        "last_processed": None,
                        "status": "Idle",
                        "type": "unknown",
                        "sequence_number": 999,
                        "_process_times": deque(maxlen=50)
                    }
        except Exception as e:
            logging.error(f"Stats init error: {e}")
    
    def update(self, model_name, status=None, rows=0, process_time=0, file_name=None, success=True):
        """Non-blocking update"""
        try:
            if not self._lock.acquire(blocking=False):
                return
            
            try:
                if model_name not in self._stats:
                    return
                
                stats = self._stats[model_name]
                
                if status:
                    stats["status"] = status
                
                if file_name:
                    stats["last_file"] = file_name
                    stats["last_processed"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                if success:
                    stats["total_files"] += 1
                    stats["total_rows"] += rows
                    
                    if process_time > 0:
                        stats["_process_times"].append(process_time)
                        if stats["_process_times"]:
                            stats["avg_process_time"] = sum(stats["_process_times"]) / len(stats["_process_times"])
                else:
                    stats["failed_files"] += 1
                
                # Auto-save periodically
                current_time = time.time()
                if current_time - self._last_save_time > self._save_interval:
                    self._save_stats()
                    self._last_save_time = current_time
                    
            finally:
                self._lock.release()
        except Exception as e:
            logging.debug(f"Stats update error: {e}")
    
    def _save_stats(self):
        """Internal method to save stats"""
        try:
            stats_to_save = {}
            for name, stats in self._stats.items():
                stats_to_save[name] = {
                    "total_files": stats["total_files"],
                    "total_rows": stats["total_rows"],
                    "failed_files": stats["failed_files"],
                    "avg_process_time": stats["avg_process_time"],
                    "last_file": stats["last_file"],
                    "last_processed": stats["last_processed"],
                    "type": stats["type"],
                    "sequence_number": stats["sequence_number"]
                }
            save_stats_to_file(stats_to_save)
        except Exception as e:
            logging.error(f"Error in _save_stats: {e}")
    
    def get_snapshot(self):
        """Get thread-safe snapshot"""
        try:
            if not self._lock.acquire(timeout=0.1):
                return {}
            
            try:
                snapshot = {}
                for name, stats in self._stats.items():
                    snapshot[name] = {
                        "total_files": stats["total_files"],
                        "total_rows": stats["total_rows"],
                        "failed_files": stats["failed_files"],
                        "avg_process_time": stats["avg_process_time"],
                        "last_file": stats["last_file"],
                        "last_processed": stats["last_processed"],
                        "status": stats["status"],
                        "type": stats["type"],
                        "sequence_number": stats["sequence_number"]
                    }
                return snapshot
            finally:
                self._lock.release()
        except Exception:
            return {}
    
    def force_save(self):
        """Force save stats immediately"""
        try:
            if self._lock.acquire(timeout=1):
                try:
                    self._save_stats()
                finally:
                    self._lock.release()
        except Exception as e:
            logging.error(f"Error in force_save: {e}")

class LockFreeLogs:
    """Thread-safe logs without blocking"""
    def __init__(self, maxlen=200):
        self._logs = deque(maxlen=maxlen)
        self._lock = RLock()
    
    def append(self, log_entry):
        """Non-blocking append"""
        try:
            if self._lock.acquire(blocking=False):
                try:
                    self._logs.append(log_entry)
                finally:
                    self._lock.release()
        except Exception:
            pass
    
    def get_snapshot(self):
        """Get thread-safe snapshot"""
        try:
            if self._lock.acquire(timeout=0.1):
                try:
                    return list(self._logs)
                finally:
                    self._lock.release()
        except Exception:
            return []

# Global instances - NOW DEFINED AFTER THE CLASSES
global_stats = LockFreeStats()
global_logs = LockFreeLogs()
config_lock = Lock()
process_times = {}
observers_dict = {}
shutdown_event = Event()
killer = GracefulKiller()
# ========================= SEQUENTIAL PROCESSOR =========================
class SequentialProcessor:
    """Sequential file processor that processes models in priority order - IMPROVED VERSION"""
    def __init__(self):
        self.running = True
        self.current_sequence = 1
        self.max_sequence = max([config.get('sequence_number', 999) for config in UNIFIED_CONFIGS.values()])
        self.processing_stats = {}
        self.processing_lock = Lock()
        self.last_activity = time.time()  # ===== ADDED: Track activity =====
        self.last_heartbeat = time.time()  # ===== ADDED: Track heartbeat logging =====
        
    def get_models_by_sequence(self):
        """Get models sorted by sequence number - UNCHANGED"""
        models_with_sequence = []
        for model_name, config in UNIFIED_CONFIGS.items():
            seq = config.get('sequence_number', 999)
            models_with_sequence.append((seq, model_name, config))
        
        models_with_sequence.sort(key=lambda x: x[0])
        return models_with_sequence
    
    def process_sequential_loop(self):
        """Main sequential processing loop - IMPROVED VERSION"""
        logging.info("🔄 Starting sequential processing loop")
        
        consecutive_idle_cycles = 0  # ===== ADDED: Track idle cycles =====
        max_idle_cycles = 10  # ===== ADDED: After 10 idle cycles, take longer break =====
        
        while self.running and not shutdown_event.is_set():
            try:
                # ===== ADDED: Check for shutdown signal every iteration =====
                if killer.kill_now:
                    logging.info("Shutdown requested in processing loop")
                    break
                
                # ===== ADDED: Heartbeat logging every 60 seconds =====
                if time.time() - self.last_heartbeat > 60:
                    logging.info(f"Currentbeat - Sequence {self.current_sequence}, Running: {self.running}, Idle cycles: {consecutive_idle_cycles}")
                    self.last_heartbeat = time.time()
                
                # Get models in sequence order
                models_in_order = self.get_models_by_sequence()
                
                any_file_processed = False  # ===== ADDED: Track if any file was processed =====
                
                for seq, model_name, config in models_in_order:
                    # ===== ADDED: Frequent shutdown checks =====
                    if not self.running or shutdown_event.is_set() or killer.kill_now:
                        break
                    
                    # Skip if sequence number is 0 or invalid
                    seq_num = config.get('sequence_number', 0)
                    if seq_num == 0:
                        logging.debug(f"⏭️ Skipping {model_name} - sequence number is 0 (disabled)")
                        continue
                    
                    if seq_num < 0:
                        logging.debug(f"⏭️ Skipping {model_name} - invalid sequence number: {seq_num}")
                        continue
                    
                    # Update current sequence for stats
                    with self.processing_lock:
                        self.current_sequence = seq
                    
                    logging.debug(f"🔍 Checking sequence {seq}: {model_name}")
                    
                    # Process one file from this model
                    file_processed = self.process_one_file_for_model(model_name, config)
                    
                    # ===== ADDED: Track activity =====
                    if file_processed:
                        any_file_processed = True
                        self.last_activity = time.time()
                        consecutive_idle_cycles = 0
                        time.sleep(0.5)  # Brief pause after processing a file
                    else:
                        time.sleep(0.5)  # Brief pause between models
                
                # ===== ADDED: Adaptive sleep based on activity =====
                if not any_file_processed:
                    consecutive_idle_cycles += 1
                    
                    if consecutive_idle_cycles >= max_idle_cycles:
                        # Been idle for a while, take longer break
                        logging.debug(f"💤 No activity for {consecutive_idle_cycles} cycles, taking 10s break")
                        
                        # Sleep in small chunks to allow quick shutdown
                        for _ in range(20):  # 20 x 0.5s = 10s
                            if shutdown_event.is_set() or killer.kill_now:
                                break
                            time.sleep(0.5)
                        
                        consecutive_idle_cycles = 0  # Reset counter
                    else:
                        time.sleep(2)  # Normal idle pause
                else:
                    time.sleep(1)  # Brief pause between cycles when active
                
            except KeyboardInterrupt:
                logging.info("⏹️ KeyboardInterrupt received in processing loop")
                break
            except Exception as e:
                logging.error(f"❌ Sequential loop error: {e}", exc_info=True)
                time.sleep(5)  # Wait before retrying
    
    def process_one_file_for_model(self, model_name, config):
        """Process one file for a specific model with timeout - IMPROVED VERSION"""
        try:
            watch_folder = config["watch_folder"]
            
            # ===== ADDED: Quick shutdown check =====
            if shutdown_event.is_set() or killer.kill_now:
                return False
            
            # Check if folder exists
            if not os.path.exists(watch_folder):
                logging.debug(f"[{model_name}] ⚠️ Watch folder not found: {watch_folder}")
                return False
            
            # Get ALL files
            files = []
            try:
                # ===== MODIFIED: Use os.scandir for better performance and add shutdown checks =====
                with os.scandir(watch_folder) as entries:
                    for entry in entries:
                        if shutdown_event.is_set() or killer.kill_now:
                            break
                        if entry.is_file():
                            files.append(entry.name)
            except Exception as e:
                logging.error(f"[{model_name}] Error listing files: {e}")
                return False
            
            if not files:
                return False
            
            # Sort files by modification time (oldest first)
            try:
                files.sort(key=lambda f: os.path.getmtime(os.path.join(watch_folder, f)))
            except Exception as e:
                logging.error(f"[{model_name}] Error sorting files: {e}")
                return False
            
            locked_files_count = 0
            non_csv_files_count = 0
            
            for file_name in files:
                # ===== ADDED: Frequent shutdown checks =====
                if not self.running or shutdown_event.is_set() or killer.kill_now:
                    return False
                
                file_path = os.path.join(watch_folder, file_name)
                
                # **IMMEDIATELY CHECK AND MOVE NON-CSV FILES**
                if check_and_move_non_csv(file_path, config, model_name):
                    non_csv_files_count += 1
                    continue
                
                # ===== MODIFIED: Check accessibility with timeout =====
                if not self.is_file_accessible(file_path, timeout=2):
                    logging.debug(f"[{model_name}] {file_name} - File is locked, skipping")
                    locked_files_count += 1
                    continue
                
                # Wait for file to be stable (5 second timeout instead of 15)
                if not wait_for_file(file_path, model_name, max_wait=5):
                    logging.warning(f"[{model_name}] {file_name} - File not stable after 5s, skipping")
                    locked_files_count += 1
                    continue
                
                # Process the file with timeout (30 second timeout)
                logging.info(f"[{model_name}] 🚀 Processing: {file_name}")
                success = self.process_file_with_timeout(file_path, config, model_name, 30)
                
                if success:
                    return True  # One file processed successfully, move to next sequence
                else:
                    # File failed processing, try next file in same model
                    continue
            
            # If we get here, we've checked all files
            if locked_files_count > 0:
                logging.debug(f"[{model_name}] ⏭️ All {locked_files_count} CSV files are locked - Moving to next sequence")
            elif non_csv_files_count > 0:
                logging.debug(f"[{model_name}] ⏭️ Only non-CSV files found ({non_csv_files_count}) - Moving to next sequence")
            
            return False
            
        except Exception as e:
            logging.error(f"[{model_name}] Error in process_one_file_for_model: {e}", exc_info=True)
            return False
        
    def is_file_accessible(self, file_path, timeout=2):
        """Check if file is not locked by another process - IMPROVED VERSION"""
        try:
            # ===== ADDED: Timeout protection =====
            with timeout_file_access(seconds=timeout):
                with open(file_path, 'r') as f:
                    return True
        except (TimeoutError, IOError, PermissionError, OSError):
            return False
    
    def process_file_with_timeout(self, file_path, config, model_name, timeout_seconds):
        """Process a file with timeout protection - UNCHANGED"""
        import threading
        
        result = {"success": False, "error": None}
        event = threading.Event()
        
        def process_worker():
            try:
                process_csv_file(file_path, config, model_name)
                result["success"] = True
            except Exception as e:
                result["error"] = str(e)
                result["success"] = False
            finally:
                event.set()
        
        # Start processing thread
        worker_thread = threading.Thread(target=process_worker, daemon=True)
        worker_thread.start()
        
        # Wait for completion or timeout
        event_occurred = event.wait(timeout=timeout_seconds)
        
        if not event_occurred:
            # Timeout occurred
            logging.error(f"[{model_name}] ⏱️ TIMEOUT processing {os.path.basename(file_path)} after {timeout_seconds}s")
            
            # Try to move the problematic file to notgood folder
            try:
                notgood_path = os.path.join(config["notgood_folder"], os.path.basename(file_path))
                if os.path.exists(file_path):
                    shutil.move(file_path, notgood_path)
                    logging.info(f"[{model_name}] Moved timed-out file to NotGood folder")
            except Exception as move_error:
                logging.error(f"[{model_name}] Failed to move timed-out file: {move_error}")
            
            return False
        
        return result["success"]
    
    def stop(self):
        """Stop the sequential processor - UNCHANGED"""
        self.running = False
        logging.info("🛑 Sequential processor stopped")

# Initialize stats for all models
for model_name, config in UNIFIED_CONFIGS.items():
    global_stats.initialize(model_name)
    global_stats._stats[model_name]["type"] = config.get("type", "unknown")
    global_stats._stats[model_name]["sequence_number"] = config.get("sequence_number", 999)

# Create sequential processor instance
sequential_processor = SequentialProcessor()

def process_cell_value(value, transform=None, file_name=None, error_code=None):
    """Process cell value based on transformation type"""
    if transform == "filename":
        return file_name if file_name else ""
    
    if transform == "zero":
        return "0"
    
    if transform == "error_code":
        return error_code if error_code else "N"
    
    if transform == "autodate":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if transform == "date":
        val_str = str(value).strip() if not pd.isna(value) else ""
        return val_str
    
    val_str = str(value).strip() if not pd.isna(value) else ""

    if transform == "equater_name":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        
        import re
        eqr_match = re.search(r'EQR[_-]?\d+', val_str, re.IGNORECASE)
        if eqr_match:
            return eqr_match.group()
        
        return val_str[-7:] if len(val_str) >= 7 else val_str

    if transform == "last33_csv":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-33:] if len(val_str) >= 33 else val_str
        return val_str + ".CSV"
    
    if transform == "last30_csv":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-30:] if len(val_str) >= 30 else val_str
        return val_str + ".CSV"

    if transform == "last34":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-34:] if len(val_str) >= 34 else val_str
        return val_str
    if transform == "last30":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-30:] if len(val_str) >= 30 else val_str
        return val_str
    
    if transform == "last29":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-29:] if len(val_str) >= 29 else val_str
        return val_str
    
    if transform == "last23":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-23:] if len(val_str) >= 23 else val_str
        return val_str

    if transform == "last20":
            if ":" in val_str:
                val_str = val_str.split(":")[-1].strip()
            else:
                val_str = val_str[-20:] if len(val_str) >= 20 else val_str
            return val_str
    
    if transform == "last28":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-28:] if len(val_str) >= 28 else val_str
        return val_str
    
    if transform == "last33":
        if ":" in val_str:
            val_str = val_str.split(":")[-1].strip()
        else:
            val_str = val_str[-33:] if len(val_str) >= 33 else val_str
        return val_str
    
    if transform == "last7":
        return val_str[-7:] if len(val_str) >= 7 else val_str
    
    return val_str


def get_error_code_from_column(df_raw, status_col_index):
    """Get error code from specified column"""
    try:
        if df_raw.shape[1] > status_col_index:
            status_column = df_raw.iloc[:, status_col_index]
            
            has_error = False
            
            # Iterate through the series with enumerate to get proper row index
            for row_idx, value in enumerate(status_column):
                if pd.isna(value):
                    continue
                    
                val_str = str(value).strip()
                
                if not val_str:
                    continue
                
                # Skip header row if it says "ERROR" (case-insensitive)
                if row_idx == 2 and status_col_index == 7 and val_str.upper() == "ERROR":
                    continue
                
                # ANY non-empty value means there's an error
                has_error = True
                logging.info(f"Error detected at row {row_idx}, column {status_col_index}: '{val_str}'")
                break
            
            return "Y" if has_error else "N"
        else:
            return "N"
    except Exception as e:
        logging.warning(f"Error checking status column: {e}")
        return "N"


def convert_data_types(data, config, model_name):
    """Convert data types according to the target table structure"""
    try:
        data_type_mapping = config.get("data_type_mapping", {})
        numeric_columns = data_type_mapping.get("numeric_columns", [])
        varchar_columns = data_type_mapping.get("varchar_columns", [])
        
        converted_data = data.copy()
        
        for col_name, value in data.items():
            if col_name in numeric_columns:
                if value == "" or value is None:
                    converted_data[col_name] = None
                else:
                    try:
                        clean_value = re.sub(r'[^\d.-]', '', str(value))
                        if clean_value and clean_value != "-" and clean_value != ".":
                            converted_data[col_name] = float(clean_value)
                        else:
                            converted_data[col_name] = None
                    except (ValueError, TypeError) as e:
                        logging.warning(f"[{model_name}] Could not convert {col_name} value '{value}' to numeric: {e}")
                        converted_data[col_name] = None
            
            elif col_name in varchar_columns:
                if value is None:
                    converted_data[col_name] = None
                else:
                    converted_data[col_name] = str(value).strip()
        
        return converted_data
        
    except Exception as e:
        logging.error(f"[{model_name}] Error in data type conversion: {e}")
        return data


# ========================= FLASK APP SETUP =========================
app = Flask(__name__)
app.secret_key = secrets.token_hex(32)

USERS = {
    'admin': generate_password_hash('admin#123'),
    'ford_user': generate_password_hash('ford123'),
    'sona_user': generate_password_hash('sona123'),
}

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'logged_in' not in session or session.get('username') != 'admin':
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated_function


# ========================= CUSTOM LOG HANDLER =========================
class WebLogHandler(logging.Handler):
    """Non-blocking log handler"""
    def emit(self, record):
        try:
            log_entry = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "level": record.levelname,
                "message": self.format(record)
            }
            global_logs.append(log_entry)
        except Exception:
            pass


# ========================= LOGGING SETUP =========================
def setup_logging(config, model_name):
    """Setup logging for each configuration"""
    log_dir = os.path.join(BASE_DIR, "logs", model_name)
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, "process.log")
    
    model_logger = logging.getLogger(model_name)
    model_logger.setLevel(logging.INFO)
    model_logger.propagate = True
    
    handler = RotatingFileHandler(
        log_file, maxBytes=5*1024*1024, backupCount=5, encoding='utf-8'
    )
    formatter = logging.Formatter(f"[{model_name}] %(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    
    handler_exists = False
    for h in model_logger.handlers:
        if isinstance(h, RotatingFileHandler) and h.baseFilename == handler.baseFilename:
            handler_exists = True
            break
    
    if not handler_exists:
        model_logger.addHandler(handler)
        logging.info(f"[{model_name}] ✅ Logger configured - writing to: {log_file}")
    
    return handler

logger = logging.getLogger()
logger.setLevel(logging.INFO)

if logger.handlers:
    logger.handlers.clear()

web_handler = WebLogHandler()
web_handler.setFormatter(logging.Formatter("%(message)s"))
logger.addHandler(web_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(console_handler)

master_log_dir = os.path.join(BASE_DIR, "logs")
os.makedirs(master_log_dir, exist_ok=True)
master_log_file = os.path.join(master_log_dir, "master.log")
master_file_handler = RotatingFileHandler(
    master_log_file, maxBytes=10*1024*1024, backupCount=10, encoding='utf-8'
)
master_file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
logger.addHandler(master_file_handler)
logging.info(f"✅ Master log file created: {master_log_file}")


# ========================= ENCODING DETECTION =========================
def detect_file_encoding(file_path):
    """Detect the encoding of a file using chardet library"""
    try:
        with open(file_path, 'rb') as f:
            raw_data = f.read(10000)
            result = chardet.detect(raw_data)
            detected_encoding = result['encoding']
            confidence = result['confidence']
            
            if detected_encoding and confidence > 0.7:
                return detected_encoding
            else:
                return None
    except Exception as e:
        logging.warning(f"Error detecting encoding: {e}")
        return None


def read_csv_with_encoding(file_path, model_name):
    """Read CSV file with automatic encoding detection"""
    file_name = os.path.basename(file_path)
    
    encodings_to_try = []
    
    detected_encoding = detect_file_encoding(file_path)
    if detected_encoding:
        encodings_to_try.append(detected_encoding)
        logging.info(f"[{model_name}] {file_name} - Detected encoding: {detected_encoding}")
    
    encodings_to_try.extend([
        'utf-8',
        'cp1252',
        'windows-1252',
        'latin1',
        'iso-8859-1',
        'utf-16'
    ])
    
    seen = set()
    encodings_to_try = [x for x in encodings_to_try if not (x in seen or seen.add(x))]
    
    last_error = None
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, encoding=encoding)
            logging.info(f"[{model_name}] {file_name} - Successfully read with encoding: {encoding}")
            return df, encoding
        except UnicodeDecodeError as e:
            last_error = e
            logging.debug(f"[{model_name}] {file_name} - Failed with {encoding}: {e}")
            continue
        except Exception as e:
            last_error = e
            logging.debug(f"[{model_name}] {file_name} - Error with {encoding}: {e}")
            continue
    
    for error_mode in ['ignore', 'replace']:
        try:
            df = pd.read_csv(file_path, encoding='latin1', encoding_errors=error_mode)
            logging.warning(f"[{model_name}] {file_name} - Read with latin1 and error mode '{error_mode}'")
            return df, f'latin1-{error_mode}'
        except Exception:
            continue
    
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        decoded = content.decode('cp1252', errors='replace')
        import io
        df = pd.read_csv(io.StringIO(decoded))
        logging.warning(f"[{model_name}] {file_name} - Read with cp1252 binary fallback")
        return df, 'cp1252-binary'
    except Exception as e:
        last_error = e
    
    logging.error(f"[{model_name}] {file_name} - All encoding attempts failed. Last error: {last_error}")
    raise Exception(f"Could not read file with any encoding. Last error: {last_error}")


def read_csv_raw_with_encoding(file_path, model_name):
    """Read CSV file without headers (for SONA type) with automatic encoding detection"""
    file_name = os.path.basename(file_path)
    
    encodings_to_try = []
    
    detected_encoding = detect_file_encoding(file_path)
    if detected_encoding:
        encodings_to_try.append(detected_encoding)
        logging.info(f"[{model_name}] {file_name} - Detected encoding: {detected_encoding}")
    
    encodings_to_try.extend([
        'utf-8',
        'cp1252',
        'windows-1252',
        'latin1',
        'iso-8859-1',
        'utf-16'
    ])
    
    seen = set()
    encodings_to_try = [x for x in encodings_to_try if not (x in seen or seen.add(x))]
    
    last_error = None
    
    for encoding in encodings_to_try:
        try:
            df = pd.read_csv(file_path, header=None, encoding=encoding)
            logging.info(f"[{model_name}] {file_name} - Successfully read with encoding: {encoding}")
            return df, encoding
        except pd.errors.ParserError as e:
            last_error = e
            logging.debug(f"[{model_name}] {file_name} - Parser error with {encoding}: {e}")
            continue
        except UnicodeDecodeError as e:
            last_error = e
            logging.debug(f"[{model_name}] {file_name} - Failed with {encoding}: {e}")
            continue
        except Exception as e:
            last_error = e
            logging.debug(f"[{model_name}] {file_name} - Error with {encoding}: {e}")
            continue
    
    logging.info(f"[{model_name}] {file_name} - Attempting to read with variable column handling")
    
    for encoding in encodings_to_try:
        try:
            lines = []
            max_cols = 0
            
            with open(file_path, 'r', encoding=encoding) as f:
                for line in f:
                    cols = [col.strip() for col in line.strip().split(',')]
                    lines.append(cols)
                    max_cols = max(max_cols, len(cols))
            
            padded_lines = []
            for line in lines:
                padded_line = line + [''] * (max_cols - len(line))
                padded_lines.append(padded_line)
            
            df = pd.DataFrame(padded_lines)
            logging.info(f"[{model_name}] {file_name} - Successfully read with variable column handling (encoding: {encoding}, max_cols: {max_cols})")
            return df, f"{encoding}-variable"
        except UnicodeDecodeError:
            continue
        except Exception as e:
            logging.debug(f"[{model_name}] {file_name} - Variable column handling failed with {encoding}: {e}")
            continue
    
    for error_mode in ['ignore', 'replace']:
        try:
            lines = []
            max_cols = 0
            
            with open(file_path, 'r', encoding='latin1', errors=error_mode) as f:
                for line in f:
                    cols = [col.strip() for col in line.strip().split(',')]
                    lines.append(cols)
                    max_cols = max(max_cols, len(cols))
            
            padded_lines = []
            for line in lines:
                padded_line = line + [''] * (max_cols - len(line))
                padded_lines.append(padded_line)
            
            df = pd.DataFrame(padded_lines)
            logging.warning(f"[{model_name}] {file_name} - Read with latin1-{error_mode} and variable column handling")
            return df, f'latin1-{error_mode}-variable'
        except Exception:
            continue
    
    try:
        with open(file_path, 'rb') as f:
            content = f.read()
        decoded = content.decode('cp1252', errors='replace')
        
        lines = []
        max_cols = 0
        for line in decoded.split('\n'):
            if line.strip():
                cols = [col.strip() for col in line.strip().split(',')]
                lines.append(cols)
                max_cols = max(max_cols, len(cols))
        
        padded_lines = []
        for line in lines:
            padded_line = line + [''] * (max_cols - len(line))
            padded_lines.append(padded_line)
        
        df = pd.DataFrame(padded_lines)
        logging.warning(f"[{model_name}] {file_name} - Read with cp1252 binary fallback")
        return df, 'cp1252-binary-variable'
    except Exception as e:
        last_error = e
    
    logging.error(f"[{model_name}] {file_name} - All encoding attempts failed. Last error: {last_error}")
    raise Exception(f"Could not read file with any encoding. Last error: {last_error}")


# ========================= FOLDER SETUP =========================
def setup_folders():
    for model_name, config in UNIFIED_CONFIGS.items():
        processed_folder = config.get("processed_folder")
        notgood_folder = config.get("notgood_folder")
        
        if not processed_folder or not os.path.exists(processed_folder):
            logging.warning(f"[{model_name}] ⚠️ Processed folder not found or not configured: {processed_folder}")
        else:
            logging.info(f"[{model_name}] ✅ Processed folder found: {processed_folder}")
        
        if not notgood_folder or not os.path.exists(notgood_folder):
            logging.warning(f"[{model_name}] ⚠️ NotGood folder not found or not configured: {notgood_folder}")
        else:
            logging.info(f"[{model_name}] ✅ NotGood folder found: {notgood_folder}")
        
        if not os.path.exists(config["watch_folder"]):
            logging.warning(f"[{model_name}] ⚠️ Watch folder not found: {config['watch_folder']}")
        else:
            logging.info(f"[{model_name}] ✅ Watch folder found: {config['watch_folder']}")
        
        config["processed_folder"] = processed_folder
        config["notgood_folder"] = notgood_folder
        
        log_dir = os.path.join(BASE_DIR, "logs", model_name)
        os.makedirs(log_dir, exist_ok=True)
        logging.info(f"[{model_name}] ✅ Log folder created/verified: {log_dir}")
        
        setup_logging(config, model_name)
        
        logging.info(f"[{model_name}] === FOLDER CONFIGURATION ===")
        logging.info(f"[{model_name}] Watch folder: {config['watch_folder']}")
        logging.info(f"[{model_name}] Processed folder (ReadFiles): {processed_folder}")
        logging.info(f"[{model_name}] Not good folder (NotGoodFiles): {notgood_folder}")
        logging.info(f"[{model_name}] Log folder: {log_dir}")
        logging.info(f"[{model_name}] Sequence number: {config.get('sequence_number', 'Not set')}")
        logging.info(f"[{model_name}] ========================")

setup_folders()


def check_available_odbc_drivers():
    """Check what ODBC drivers are available on this system"""
    try:
        drivers = [x for x in pyodbc.drivers()]
        logging.info("=" * 60)
        logging.info("AVAILABLE ODBC DRIVERS ON THIS SYSTEM:")
        logging.info("=" * 60)
        for i, driver in enumerate(drivers, 1):
            logging.info(f"  {i}. {driver}")
        logging.info("=" * 60)
        
        sql_drivers = [d for d in drivers if 'SQL Server' in d]
        if sql_drivers:
            logging.info(f"✅ SQL Server drivers found: {sql_drivers}")
            recommended = sql_drivers[-1]
            logging.info(f"💡 Recommended driver: {recommended}")
        else:
            logging.error("❌ No SQL Server ODBC drivers found!")
            logging.error("   Please install 'ODBC Driver 17 for SQL Server'")
        logging.info("=" * 60)
        
        return drivers
    except Exception as e:
        logging.error(f"Error checking ODBC drivers: {e}")
        return []


# ========================= DATABASE HELPERS =========================
def test_database_connection(driver, server, database, username, password):
    """Test database connection before creating model"""
    try:
        conn_str = f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Connection Timeout=10;"
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            return True, "✅ Connection successful"
    except pyodbc.OperationalError as e:
        return False, f"❌ Network/Database error: {str(e)}"
    except pyodbc.InterfaceError as e:
        return False, f"❌ Driver/Connection error: {str(e)}"
    except pyodbc.DatabaseError as e:
        return False, f"❌ Database error: {str(e)}"
    except Exception as e:
        return False, f"❌ Unexpected error: {str(e)}"


def create_ford_table(table_name, conn_str):
    """Create table for FORD models"""
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            CREATE TABLE {table_name} (
                Id INT IDENTITY(1,1) PRIMARY KEY,
                FileName NVARCHAR(255),
                MachineName NVARCHAR(100),
                DateTime DATETIME,
                HousingNo NVARCHAR(255),
                Characteristic NVARCHAR(255),
                Actual FLOAT,
                Nominal FLOAT,
                UpperTol FLOAT,
                LowerTol FLOAT,
                Deviation FLOAT,
                Status NVARCHAR(5),
                ProcessedAt DATETIME DEFAULT GETDATE()
            )
            """)
            conn.commit()
        logging.info(f"✅ Table {table_name} checked/created successfully.")
    except Exception as e:
        logging.error(f"❌ Error creating/checking table {table_name}: {e}")


def create_sona_table(table_name, conn_str, columns):
    """Check if SONA table exists - DO NOT CREATE OR MODIFY"""
    try:
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{table_name}'
            """)
            
            table_exists = cursor.fetchone()[0] > 0
            
            if table_exists:
                logging.info(f"✅ Table {table_name} exists - ready for data insertion")
                return True
            else:
                logging.error(f"❌ Table {table_name} does not exist - please create it manually")
                return False
                
    except Exception as e:
        logging.error(f"❌ Error checking table {table_name}: {e}")
        return False


def debug_database_connection(config, model_name):
    """Debug database connection and table status"""
    try:
        conn_str = build_connection_string(
            config["db_driver"], config["db_server"], 
            config["db_name"], config["db_username"], config["db_password"]
        )
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT DB_NAME()")
            current_db = cursor.fetchone()[0]
            logging.info(f"🔍 [{model_name}] Connected to database: {current_db}")
            
            cursor.execute("""
                SELECT TABLE_NAME 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_TYPE = 'BASE TABLE'
                ORDER BY TABLE_NAME
            """)
            tables = cursor.fetchall()
            logging.info(f"🔍 [{model_name}] Tables in database: {[t[0] for t in tables]}")
            
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = '{config['table_name']}'
            """)
            table_exists = cursor.fetchone()[0] > 0
            logging.info(f"🔍 [{model_name}] Table '{config['table_name']}' exists: {table_exists}")
            
    except Exception as e:
        logging.error(f"❌ [{model_name}] Debug connection failed: {e}")

# ========================= TIMEOUT PROTECTION =========================
@contextmanager
def timeout_file_access(seconds=5):
    """Context manager for file operations with timeout - ONLY FOR UNIX/LINUX"""
    # Check if we're on Windows
    if sys.platform == 'win32':
        # Windows doesn't support SIGALRM, just yield without timeout
        yield
        return
    
    def timeout_handler(signum, frame):
        raise TimeoutError("File operation timed out")
    
    # Set the signal handler and alarm (only on Unix)
    old_handler = signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(seconds)
    
    try:
        yield
    finally:
        signal.alarm(0)  # Disable alarm
        signal.signal(signal.SIGALRM, old_handler)

# ========================= UTILITY FUNCTIONS =========================
def clean_datetime_string(val):
    if pd.isna(val):
        return None
    val = str(val).replace("\ufeff", "").strip()
    val = re.sub(r"\s+", " ", val)
    return val


def try_parse_datetime(val):
    if not val:
        return None
    val = clean_datetime_string(val)
    
    possible_formats = [
        "%d-%m-%Y %H:%M:%S", "%d-%m-%Y %H:%M",
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M",
        "%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M",
        "%d-%b-%Y %H:%M:%S", "%d-%b-%Y %H:%M",
    ]
    
    for fmt in possible_formats:
        try:
            return datetime.strptime(val, fmt)
        except ValueError:
            continue
    
    try:
        return parser.parse(val, dayfirst=True)
    except Exception:
        return None


def is_file_locked(file_path, timeout=3):
    """Check if file is locked - WITH TIMEOUT"""
    try:
        with timeout_file_access(seconds=timeout):
            with open(file_path, "a"):
                return False
    except (TimeoutError, IOError, OSError, PermissionError):
        return True


def wait_for_file(file_path, model_name, max_wait=15):
    """Wait for file to be stable and accessible"""
    file_name = os.path.basename(file_path)
    wait_count = 0
    
    while wait_count < max_wait:
        # Check shutdown event frequently
        if shutdown_event.is_set() or killer.kill_now:
            logging.info(f"[{model_name}] {file_name} - Shutdown requested, stopping file wait")
            return False
        
        if not os.path.exists(file_path):
            logging.error(f"[{model_name}] {file_name} - File disappeared")
            return False
        
        try:
            # Use timeout for file access
            with timeout_file_access(seconds=3):
                with open(file_path, "r+b") as f:
                    pass
            
            size1 = os.path.getsize(file_path)
            time.sleep(0.3)
            
            if not os.path.exists(file_path):
                return False
            
            size2 = os.path.getsize(file_path)
            
            if size1 == size2 and size1 > 0:
                return True
            
            wait_count += 1
            time.sleep(1)
            
        except TimeoutError:
            logging.warning(f"[{model_name}] {file_name} - File access timed out, attempt {wait_count}/{max_wait}")
            wait_count += 1
            time.sleep(1)
            
        except (IOError, PermissionError, OSError) as e:
            logging.debug(f"[{model_name}] {file_name} - File locked: {e}")
            wait_count += 1
            time.sleep(1)
    
    logging.warning(f"[{model_name}] {file_name} - Failed to access after {max_wait} attempts")
    return False


def safe_move(src, dst):
    try:
        # Copy to destination (overwrites if exists)
        shutil.copy2(src, dst)
        
        # Only delete source if copy was successful
        if os.path.exists(dst):
            os.remove(src)
            
    except Exception as e:
        logging.warning(f"⚠️ Could not move {src} to {dst}: {e}")


def update_stats(model_name, status, rows=0, process_time=0, file_name=None, success=True):
    """Non-blocking stats update"""
    global_stats.update(model_name, status, rows, process_time, file_name, success)

def build_connection_string(driver, server, database, username, password):
    """Build connection string with connection timeout"""
    return f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};UID={username};PWD={password};Connection Timeout=10;"


def get_logger(model_name):
    """Get the appropriate logger for a model"""
    if model_name:
        return logging.getLogger(model_name)
    return logging.getLogger()


# ========================= FORD CSV PROCESSOR =========================
def process_ford_csv(file_path, config, model_name):
    """Process FORD model CSVs with UPDATE/INSERT logic (row-based with headers)"""
    file_name = os.path.basename(file_path)
    start_time = time.time()
    
    update_stats(model_name, f"Processing: {file_name}")
    
    if not file_name.lower().endswith(".csv"):
        logging.warning(f"[{model_name}] {file_name} - Not a CSV file")
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    if not file_name.startswith(config["file_prefix"]):
        logging.error(f"[{model_name}] {file_name} - Invalid file prefix")
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    if not wait_for_file(file_path, model_name):
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    try:
        df, encoding_used = read_csv_with_encoding(file_path, model_name)
    except Exception as e:
        logging.error(f"[{model_name}] {file_name} - Failed to read: {e}")
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    if not all(col in df.columns for col in config["required_columns"]):
        logging.error(f"[{model_name}] {file_name} - Missing columns")
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    df["DATE_TIME_RAW"] = df["DATE_TIME"].apply(clean_datetime_string)
    df["DATE_TIME"] = df["DATE_TIME_RAW"].apply(try_parse_datetime)
    
    invalid_rows = df["DATE_TIME"].isna().sum()
    if invalid_rows > 0:
        logging.warning(f"[{model_name}] {file_name} - {invalid_rows} invalid datetime rows skipped")
    
    df = df.dropna(subset=["DATE_TIME"])
    if df.empty:
        logging.error(f"[{model_name}] {file_name} - No valid data")
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)
        return
    
    df["Status"] = df.apply(
        lambda r: "OK" if (r["Actual"] >= (r["Nominal"] + r["Lower Tol"]) and 
                          r["Actual"] <= (r["Nominal"] + r["Upper Tol"])) else "NG",
        axis=1
    )
    
    machine_name = str(df.iloc[0]["MACHINENO"])
    inserted_count = 0
    updated_count = 0
    
    try:
        conn_str = build_connection_string(
            config["db_driver"], config["db_server"], 
            config["db_name"], config["db_username"], config["db_password"]
        )
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            for idx, row in df.iterrows():
                try:
                    # Determine which columns to use for unique identification
                    # Priority: HousingNo > PART_NUMBER > PartNo
                    housing_no = None
                    housing_column = None
                    
                    for col_name in ["HousingNo", "PART_NUMBER", "PartNo", "Part_Number"]:
                        if col_name in row and not pd.isna(row[col_name]):
                            housing_no = str(row[col_name])
                            housing_column = col_name
                            break
                    
                    if not housing_no:
                        logging.warning(f"[{model_name}] Row {idx}: No housing identifier found, skipping")
                        continue
                    
                    # Get characteristic column
                    characteristic = None
                    characteristic_column = None
                    
                    for col_name in ["Characteristic", "Parameter", "Measurement"]:
                        if col_name in row and not pd.isna(row[col_name]):
                            characteristic = str(row[col_name])
                            characteristic_column = col_name
                            break
                    
                    if not characteristic:
                        logging.warning(f"[{model_name}] Row {idx}: No characteristic found, skipping")
                        continue
                    
                    # Check if this specific record exists (unique combination)
                    cursor.execute(f"""
                        SELECT COUNT(*) 
                        FROM {config['table_name']} 
                        WHERE FileName = ? AND HousingNo = ? AND Characteristic = ?
                    """, (file_name, housing_no, characteristic))
                    
                    exists = cursor.fetchone()[0] > 0
                    
                    if exists:
                        # UPDATE existing record
                        update_query = f"""
                            UPDATE {config['table_name']}
                            SET MachineName = ?,
                                DateTime = ?,
                                Actual = ?,
                                Nominal = ?,
                                UpperTol = ?,
                                LowerTol = ?,
                                Deviation = ?,
                                Status = ?,
                                ProcessedAt = GETDATE()
                            WHERE FileName = ? AND HousingNo = ? AND Characteristic = ?
                        """
                        
                        update_params = [
                            machine_name, 
                            row["DATE_TIME"], 
                            row.get("Actual"),
                            row.get("Nominal"),
                            row.get("Upper Tol") or row.get("UpperTol"),
                            row.get("Lower Tol") or row.get("LowerTol"),
                            row.get("Deviation"),
                            row["Status"],
                            file_name, 
                            housing_no, 
                            characteristic
                        ]
                        
                        cursor.execute(update_query, update_params)
                        updated_count += 1
                        
                    else:
                        # INSERT new record
                        insert_query = f"""
                            INSERT INTO {config['table_name']}
                            (FileName, MachineName, DateTime, HousingNo, Characteristic, Actual, Nominal, UpperTol, LowerTol, Deviation, Status)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """
                        
                        insert_params = [
                            file_name, 
                            machine_name, 
                            row["DATE_TIME"], 
                            housing_no,
                            characteristic,
                            row.get("Actual"),
                            row.get("Nominal"),
                            row.get("Upper Tol") or row.get("UpperTol"),
                            row.get("Lower Tol") or row.get("LowerTol"),
                            row.get("Deviation"),
                            row["Status"]
                        ]
                        
                        cursor.execute(insert_query, insert_params)
                        inserted_count += 1
                        
                except Exception as e:
                    logging.error(f"[{model_name}] Row {idx} processing error: {e}")
                    logging.error(f"[{model_name}] Row data: {row.to_dict()}")
                    continue
                    
            conn.commit()
        
        process_time = time.time() - start_time
        total_processed = inserted_count + updated_count
        
        if total_processed > 0:
            logging.info(f"[{model_name}] {file_name} - ✅ Processed {total_processed} rows ({inserted_count} inserted, {updated_count} updated) in {process_time:.2f}s (encoding: {encoding_used})")
            safe_move(file_path, os.path.join(config["processed_folder"], file_name))
            update_stats(model_name, "Idle", rows=total_processed, process_time=process_time, 
                        file_name=file_name, success=True)
        else:
            logging.error(f"[{model_name}] {file_name} - No rows processed")
            safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            
    except Exception as e:
        logging.error(f"[{model_name}] {file_name} - ❌❌❌ DB ERROR: {e}")
        logging.error(f"[{model_name}] Error type: {type(e).__name__}")
        logging.error(f"[{model_name}] Full traceback:", exc_info=True)
        safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
        update_stats(model_name, "Idle", file_name=file_name, success=False)

def verify_file_paths(file_path, config, model_name):
    """Verify all paths and permissions before processing"""
    file_name = os.path.basename(file_path)
    model_logger = logging.getLogger(model_name)
    
    model_logger.info(f"[{model_name}] === FILE PATH VERIFICATION ===")
    model_logger.info(f"[{model_name}] Source file: {file_path}")
    model_logger.info(f"[{model_name}] File exists: {os.path.exists(file_path)}")
    
    if os.path.exists(file_path):
        try:
            file_size = os.path.getsize(file_path)
            model_logger.info(f"[{model_name}] File size: {file_size} bytes")
            
            with open(file_path, 'r') as f:
                first_line = f.readline()
                model_logger.info(f"[{model_name}] File is readable: Yes")
                model_logger.info(f"[{model_name}] First line length: {len(first_line)} chars")
        except Exception as e:
            model_logger.error(f"[{model_name}] Error reading file: {e}")
    else:
        model_logger.error(f"[{model_name}] FILE DOES NOT EXIST!")
    
    model_logger.info(f"[{model_name}] Processed folder: {config['processed_folder']}")
    model_logger.info(f"[{model_name}] Processed folder exists: {os.path.exists(config['processed_folder'])}")
    
    if os.path.exists(config['processed_folder']):
        try:
            test_file = os.path.join(config['processed_folder'], '.write_test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            model_logger.info(f"[{model_name}] Processed folder is writable: Yes")
        except Exception as e:
            model_logger.error(f"[{model_name}] Processed folder write test failed: {e}")
    
    model_logger.info(f"[{model_name}] NotGood folder: {config['notgood_folder']}")
    model_logger.info(f"[{model_name}] NotGood folder exists: {os.path.exists(config['notgood_folder'])}")
    
    if os.path.exists(config['notgood_folder']):
        try:
            test_file = os.path.join(config['notgood_folder'], '.write_test')
            with open(test_file, 'w') as f:
                f.write('test')
            os.remove(test_file)
            model_logger.info(f"[{model_name}] NotGood folder is writable: Yes")
        except Exception as e:
            model_logger.error(f"[{model_name}] NotGood folder write test failed: {e}")
    
    model_logger.info(f"[{model_name}] === END VERIFICATION ===")


# ========================= SONA CSV PROCESSOR =========================
def process_sona_csv(file_path, config, model_name):
    """Process SONA model CSVs - UPDATE if HousingNo exists, INSERT if new
    Supports dual table insertion with custom column mapping"""
    
    file_name = os.path.basename(file_path)
    start_time = time.time()
    
    model_logger = logging.getLogger(model_name)
    
    try:
        model_logger.info(f"[{model_name}] {file_name} - 🔵 Starting processing...")
        update_stats(model_name, f"Processing: {file_name}")
        
        verify_file_paths(file_path, config, model_name)

        if not file_name.lower().endswith(".csv"):
            model_logger.warning(f"[{model_name}] {file_name} - Not a CSV")
            safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            return
        
        if not re.match(config["filename_pattern"], file_name):
            model_logger.warning(f"[{model_name}] {file_name} - Pattern mismatch")
            safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            return
        
        stabilization_delay = config.get("file_stabilization_delay", 1)
        model_logger.info(f"[{model_name}] {file_name} - Waiting {stabilization_delay}s...")
        time.sleep(stabilization_delay)
        
        if not os.path.exists(file_path):
            model_logger.error(f"[{model_name}] {file_name} - File disappeared!")
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            return
        
        if not wait_for_file(file_path, model_name, max_wait=60):
            model_logger.error(f"[{model_name}] {file_name} - Failed to acquire lock")
            safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            return
        
        model_logger.info(f"[{model_name}] {file_name} - 📖 Reading CSV...")
        df_raw, encoding_used = read_csv_raw_with_encoding(file_path, model_name)
        model_logger.info(f"[{model_name}] {file_name} - Read: {encoding_used}, shape: {df_raw.shape}")
        
        status_col_idx = config.get("status_column_index", 7)
        error_code = get_error_code_from_column(df_raw, status_col_idx)
        model_logger.info(f"[{model_name}] {file_name} - ErrorCode: {error_code}")
        
        data = {}
        missing_cells = []
        
        model_logger.info(f"[{model_name}] {file_name} - Extracting data...")
        for col_info in config["cell_mapping"]:
            row_idx = col_info[0]
            col_idx = col_info[1]
            col_name = col_info[2]
            transform = col_info[3] if len(col_info) > 3 else None
            
            if row_idx is None or col_idx is None:
                processed_val = process_cell_value(
                    None, 
                    transform=transform, 
                    file_name=file_name,
                    error_code=error_code
                )
                data[col_name] = processed_val
                continue
            
            try:
                val = df_raw.iat[row_idx, col_idx]
                processed_val = process_cell_value(
                    val, 
                    transform=transform, 
                    file_name=file_name,
                    error_code=error_code
                )
                
                if processed_val == "" and transform not in ["zero", "filename", "error_code", "autodate"]:
                    missing_cells.append(f"{col_name}({row_idx+1},{col_idx+1})")
                
                data[col_name] = processed_val
            except Exception as e:
                missing_cells.append(f"{col_name}({row_idx+1},{col_idx+1})")
                model_logger.warning(f"[{model_name}] Error reading {col_name}: {e}")
        
        if missing_cells:
            model_logger.error(f"[{model_name}] {file_name} - Missing: {', '.join(missing_cells)}")
            safe_move(file_path, os.path.join(config["notgood_folder"], file_name))
            update_stats(model_name, "Idle", file_name=file_name, success=False)
            return
        
        model_logger.info(f"[{model_name}] {file_name} - Converting data types...")
        data["ErrorCode"] = error_code
        data = convert_data_types(data, config, model_name)
        
        # Convert date string to proper datetime for SQL Server
        if "InsertDate" in data and data["InsertDate"]:
            data["InsertDate"] = convert_to_sql_datetime(data["InsertDate"], model_name, file_name)
        
        model_logger.info(f"[{model_name}] {file_name} - Connecting to database...")
        conn_str = build_connection_string(
            config["db_driver"], config["db_server"], 
            config["db_name"], config["db_username"], config["db_password"]
        )
        
        with pyodbc.connect(conn_str) as conn:
            cursor = conn.cursor()
            
            # Get HousingNo for duplicate check
            housing_no = data.get("HousingNo", "").strip()
            
            if not housing_no:
                model_logger.error(f"[{model_name}] {file_name} - HousingNo is empty or missing!")
                raise Exception("HousingNo is required but not found in data")
            
            model_logger.info(f"[{model_name}] {file_name} - Checking HousingNo: {housing_no}")
            
            # ========== CHECK PRIMARY TABLE FOR DUPLICATES ==========
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM {config['table_name']} 
                WHERE HousingNo = ?
            """, (housing_no,))
            
            exists_primary = cursor.fetchone()[0] > 0
            
            if exists_primary:
                model_logger.info(f"[{model_name}] {file_name} - ⚠️ DUPLICATE in primary table!")
            else:
                model_logger.info(f"[{model_name}] {file_name} - ✅ NEW PART in primary table!")
            
            # ========== PRIMARY TABLE OPERATION ==========
            columns = []
            params = []
            
            for col_name, value in data.items():
                columns.append(col_name)
                
                if config.get("data_type_mapping", {}).get("numeric_columns") and col_name in config["data_type_mapping"]["numeric_columns"]:
                    if value == "" or value is None:
                        params.append(None)
                    else:
                        try:
                            params.append(float(value))
                        except (ValueError, TypeError):
                            params.append(None)
                else:
                    params.append(str(value) if value is not None else None)
            
            if exists_primary:
                # UPDATE primary table
                update_columns = [col for col in columns if col != "HousingNo"]
                set_clause = ", ".join([f"{col} = ?" for col in update_columns])
                
                update_query = f"""
                    UPDATE {config['table_name']} 
                    SET {set_clause}
                    WHERE HousingNo = ?
                """
                
                update_params = [params[columns.index(col)] for col in update_columns]
                update_params.append(housing_no)
                
                model_logger.info(f"[{model_name}] {file_name} - 🔄 UPDATING Primary Table '{config['table_name']}'...")
                cursor.execute(update_query, update_params)
                action_primary = "UPDATED"
                
            else:
                # INSERT into primary table
                cols_str = ",".join(columns)
                placeholders = ",".join(["?"] * len(columns))
                insert_query = f"INSERT INTO {config['table_name']} ({cols_str}) VALUES ({placeholders})"
                
                model_logger.info(f"[{model_name}] {file_name} - ➕ INSERTING into Primary Table '{config['table_name']}'...")
                cursor.execute(insert_query, params)
                action_primary = "INSERTED"
            
            model_logger.info(f"[{model_name}] {file_name} - ✅ Primary table {action_primary}")
            
            # ========== SECONDARY TABLE OPERATION (if enabled) ==========
            secondary_config = config.get("secondary_table")
            if secondary_config and secondary_config.get("enabled", False):
                secondary_table = secondary_config["table_name"]
                column_mapping = secondary_config["column_mapping"]
                
                model_logger.info(f"[{model_name}] {file_name} - 🔄 Processing secondary table '{secondary_table}'...")
                
                # Check if HousingNo exists in secondary table
                cursor.execute(f"""
                    SELECT COUNT(*) 
                    FROM {secondary_table} 
                    WHERE HousingNo = ?
                """, (housing_no,))
                
                exists_secondary = cursor.fetchone()[0] > 0
                
                if exists_secondary:
                    model_logger.info(f"[{model_name}] {file_name} - ⚠️ DUPLICATE in secondary table!")
                else:
                    model_logger.info(f"[{model_name}] {file_name} - ✅ NEW PART in secondary table!")
                
                # Prepare data for secondary table using column mapping
                secondary_columns = []
                secondary_params = []
                
                for sec_col_name, primary_col_name in column_mapping.items():
                    secondary_columns.append(sec_col_name)
                    
                    if primary_col_name in data:
                        value = data[primary_col_name]
                        
                        # Apply same type conversion based on PRIMARY column name
                        if config.get("data_type_mapping", {}).get("numeric_columns") and primary_col_name in config["data_type_mapping"]["numeric_columns"]:
                            if value == "" or value is None:
                                secondary_params.append(None)
                            else:
                                try:
                                    secondary_params.append(float(value))
                                except (ValueError, TypeError):
                                    secondary_params.append(None)
                        else:
                            secondary_params.append(str(value) if value is not None else None)
                    else:
                        model_logger.warning(f"[{model_name}] {file_name} - Column '{primary_col_name}' not found in primary data for secondary table mapping!")
                        secondary_params.append(None)
                
                model_logger.info(f"[{model_name}] {file_name} - Secondary table columns: {secondary_columns}")
                
                if exists_secondary:
                    # UPDATE secondary table
                    update_columns_sec = [col for col in secondary_columns if col != "HousingNo"]
                    set_clause_sec = ", ".join([f"{col} = ?" for col in update_columns_sec])
                    
                    update_query_sec = f"""
                        UPDATE {secondary_table} 
                        SET {set_clause_sec}
                        WHERE HousingNo = ?
                    """
                    
                    update_params_sec = [secondary_params[secondary_columns.index(col)] for col in update_columns_sec]
                    update_params_sec.append(housing_no)
                    
                    model_logger.info(f"[{model_name}] {file_name} - 🔄 UPDATING Secondary Table...")
                    cursor.execute(update_query_sec, update_params_sec)
                    action_secondary = "UPDATED"
                    
                else:
                    # INSERT into secondary table
                    cols_str_sec = ",".join(secondary_columns)
                    placeholders_sec = ",".join(["?"] * len(secondary_columns))
                    insert_query_sec = f"INSERT INTO {secondary_table} ({cols_str_sec}) VALUES ({placeholders_sec})"
                    
                    model_logger.info(f"[{model_name}] {file_name} - ➕ INSERTING into Secondary Table...")
                    cursor.execute(insert_query_sec, secondary_params)
                    action_secondary = "INSERTED"
                
                model_logger.info(f"[{model_name}] {file_name} - ✅ Secondary table {action_secondary}")
            
            # Commit all changes
            conn.commit()
            model_logger.info(f"[{model_name}] {file_name} - ✅✅ All database operations committed - HousingNo: {housing_no}")
            
            process_time = time.time() - start_time
            processed_path = os.path.join(config["processed_folder"], file_name)
            
            model_logger.info(f"[{model_name}] {file_name} - 📦 Moving to ReadFiles...")
            
            try:
                if os.path.exists(processed_path):
                    os.remove(processed_path)
                
                shutil.move(file_path, processed_path)
                model_logger.info(f"[{model_name}] {file_name} - ✅✅✅ SUCCESS!")
                model_logger.info(f"[{model_name}] {file_name} - Primary: {action_primary} | Secondary: {action_secondary if secondary_config and secondary_config.get('enabled') else 'DISABLED'} | HousingNo: {housing_no} | Time: {process_time:.2f}s")
                
            except Exception as move_error:
                model_logger.error(f"[{model_name}] {file_name} - ❌ Move error: {move_error}")
            
            update_stats(model_name, "Idle", rows=1, process_time=process_time, 
                        file_name=file_name, success=True)
        
    except Exception as e:
        model_logger.error(f"[{model_name}] {file_name} - ❌❌❌ ERROR: {e}")
        model_logger.error(f"[{model_name}] {file_name} - Type: {type(e).__name__}")
        model_logger.error(f"[{model_name}] {file_name} - Traceback:", exc_info=True)
        
        notgood_path = os.path.join(config["notgood_folder"], file_name)
        
        try:
            if os.path.exists(file_path):
                if os.path.exists(notgood_path):
                    os.remove(notgood_path)
                shutil.move(file_path, notgood_path)
                model_logger.info(f"[{model_name}] {file_name} - Moved to NotGoodFiles")
        except Exception as move_error:
            model_logger.error(f"[{model_name}] {file_name} - Move error: {move_error}")
        
        update_stats(model_name, "Idle", file_name=file_name, success=False)

# ========================= DATE CONVERSION HELPER =========================
def convert_to_sql_datetime(date_string, model_name, file_name):
    """Convert date string to SQL Server compatible datetime format"""
    if not date_string or pd.isna(date_string):
        return None
    
    try:
        # Clean the string
        date_string = str(date_string).strip()
        
        # Try common date formats
        date_formats = [
            "%d-%m-%Y %H:%M:%S",  # 18-10-2025 15:30:45
            "%d-%m-%Y %H:%M",      # 18-10-2025 15:30
            "%Y-%m-%d %H:%M:%S",   # 2025-10-18 15:30:45
            "%Y-%m-%d %H:%M",      # 2025-10-18 15:30
            "%d/%m/%Y %H:%M:%S",   # 18/10/2025 15:30:45
            "%d/%m/%Y %H:%M",      # 18/10/2025 15:30
            "%d-%m-%Y",            # 18-10-2025
            "%Y-%m-%d",            # 2025-10-18
            "%d/%m/%Y",            # 18/10/2025
        ]
        
        for fmt in date_formats:
            try:
                dt = datetime.strptime(date_string, fmt)
                # Return in SQL Server format: YYYY-MM-DD HH:MM:SS
                return dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue
        
        # If no format matched, try dateutil parser
        try:
            dt = parser.parse(date_string, dayfirst=True)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except:
            pass
        
        # If all else fails, log and return None
        logging.warning(f"[{model_name}] {file_name} - Could not parse date: '{date_string}'")
        return None
        
    except Exception as e:
        logging.error(f"[{model_name}] {file_name} - Date conversion error: {e}")
        return None


# ========================= UNIFIED PROCESSOR =========================
def process_csv_file(file_path, config, model_name):
    """Route to appropriate processor based on model type"""
    if config["type"] == "ford":
        process_ford_csv(file_path, config, model_name)
    elif config["type"] == "sona":
        process_sona_csv(file_path, config, model_name)
    else:
        logging.error(f"[{model_name}] Unknown type: {config['type']}")

def check_and_move_non_csv(file_path, config, model_name):
    """Immediately move non-CSV files to NotGood folder - DON'T COUNT AS FAILED"""
    file_name = os.path.basename(file_path)
    
    # Check if file extension is not .csv or .CSV
    if not file_name.lower().endswith('.csv'):
        try:
            notgood_path = os.path.join(config["notgood_folder"], file_name)
            
            # Ensure NotGood folder exists
            os.makedirs(config["notgood_folder"], exist_ok=True)
            
            # Remove if already exists in NotGood
            if os.path.exists(notgood_path):
                os.remove(notgood_path)
            
            # Move file to NotGood folder
            shutil.move(file_path, notgood_path)
            
            logging.warning(f"[{model_name}] ⚠️ Non-CSV file moved to NotGood: {file_name}")
            # DON'T update stats with success=False - just set status to Idle
            update_stats(model_name, "Idle", file_name=file_name, success=True)
            return True  # File was non-CSV and moved
            
        except Exception as e:
            logging.error(f"[{model_name}] Error moving non-CSV file {file_name}: {e}")
            return True  # Still return True to skip processing
    
    return False  # File is CSV, continue processing
# ========================= FILE WATCHER =========================
class UnifiedWatcher(FileSystemEventHandler):
    """File watcher that detects new files but doesn't process them immediately"""
    def __init__(self, config, model_name):
        self.config = config
        self.model_name = model_name
        self.last_event_time = {}
        self.debounce_seconds = 2
        self.event_lock = Lock()
    
    def on_created(self, event):
        """Log file creation and immediately move non-CSV files"""
        if event.is_directory or shutdown_event.is_set():
            return
        
        file_path = event.src_path
        file_name = os.path.basename(file_path)
        
        # **IMMEDIATELY MOVE NON-CSV FILES**
        if not file_name.lower().endswith('.csv'):
            logging.warning(f"[{self.model_name}] 🔔 NON-CSV FILE DETECTED: {file_name} - Moving to NotGood immediately")
            time.sleep(1)  # Brief wait for file to be fully created
            
            if os.path.exists(file_path):
                check_and_move_non_csv(file_path, self.config, self.model_name)
            return
        
        logging.info(f"[{self.model_name}] 🔔 NEW CSV FILE DETECTED: {file_name}")
        logging.info(f"[{self.model_name}] File will be processed in sequence order")


def start_watcher_for_model(model_name, config):
    """Start file watcher for a specific model (for monitoring only)"""
    observer = Observer()
    observer.schedule(UnifiedWatcher(config, model_name), config["watch_folder"], recursive=False)
    observer.start()
    observers_dict[model_name] = observer
    logging.info(f"[{model_name}] 👀 Monitoring folder: {config['watch_folder']}")
    return observer


# ========================= WEB TEMPLATES =========================
LOGIN_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Login - Unified CSV Monitor</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #0f1419;
            background-image: 
                linear-gradient(rgba(59, 130, 246, 0.03) 1px, transparent 1px),
                linear-gradient(90deg, rgba(59, 130, 246, 0.03) 1px, transparent 1px);
            background-size: 50px 50px;
            display: flex;
            justify-content: center;
            align-items: center;
            min-height: 100vh;
            color: #e0e0e0;
        }
        .login-container {
            background: linear-gradient(145deg, #1a1f2e, #151a27);
            border: 2px solid #2d3748;
            border-radius: 15px;
            padding: 40px;
            box-shadow: 0 8px 32px rgba(59, 130, 246, 0.2);
            width: 100%;
            max-width: 400px;
        }
        h1 {
            color: #3b82f6;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2em;
            text-shadow: 0 0 20px rgba(59, 130, 246, 0.3);
            text-transform: uppercase;
            letter-spacing: 2px;
        }
        .form-group {
            margin-bottom: 20px;
        }
        label {
            display: block;
            color: #8b92a7;
            margin-bottom: 8px;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.9em;
            letter-spacing: 0.5px;
        }
        input[type="text"],
        input[type="password"] {
            width: 100%;
            padding: 12px 15px;
            background: #0f1419;
            border: 2px solid #2d3748;
            border-radius: 8px;
            color: #e5e7eb;
            font-size: 1em;
            transition: all 0.3s;
        }
        input:focus {
            outline: none;
            border-color: #3b82f6;
            box-shadow: 0 0 15px rgba(59, 130, 246, 0.3);
        }
        .btn-login {
            width: 100%;
            padding: 12px;
            background: linear-gradient(145deg, #3b82f6, #2563eb);
            border: none;
            border-radius: 8px;
            color: white;
            font-size: 1.1em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 1px;
            cursor: pointer;
            transition: all 0.3s;
            margin-top: 10px;
        }
        .btn-login:hover {
            background: linear-gradient(145deg, #2563eb, #1d4ed8);
            transform: translateY(-2px);
            box-shadow: 0 5px 20px rgba(59, 130, 246, 0.4);
        }
        .error-message {
            background: rgba(239, 68, 68, 0.1);
            border: 1px solid #ef4444;
            border-left: 4px solid #ef4444;
            color: #ef4444;
            padding: 12px 15px;
            border-radius: 8px;
            margin-bottom: 20px;
            font-size: 0.9em;
        }
        .lock-icon {
            text-align: center;
            font-size: 3em;
            margin-bottom: 20px;
            color: #3b82f6;
            text-shadow: 0 0 20px rgba(59, 130, 246, 0.5);
        }
    </style>
</head>
<body>
    <div class="login-container">
        <div class="lock-icon">🔒</div>
        <h1>Login</h1>
        {% if error %}
        <div class="error-message">{{ error }}</div>
        {% endif %}
        <form method="POST">
            <div class="form-group">
                <label for="username">Username</label>
                <input type="text" id="username" name="username" required>
            </div>
            <div class="form-group">
                <label for="password">Password</label>
                <input type="password" id="password" name="password" required>
            </div>
            <button type="submit" class="btn-login">Login</button>
        </form>
    </div>
</body>
</html>
"""

DASHBOARD_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>Sequential CSV Processor Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        :root {
            --bg-primary: #0a0e1a;
            --bg-secondary: #111827;
            --bg-card: #1a1f2e;
            --bg-card-hover: #222938;
            --border-primary: #2d3748;
            --border-accent: #3b82f6;
            --text-primary: #e5e7eb;
            --text-secondary: #9ca3af;
            --text-muted: #6b7280;
            --accent-blue: #3b82f6;
            --accent-green: #10b981;
            --accent-yellow: #fbbf24;
            --accent-red: #ef4444;
            --accent-purple: #8b5cf6;
            --accent-orange: #f59e0b;
            --shadow-sm: 0 2px 8px rgba(0, 0, 0, 0.3);
            --shadow-md: 0 4px 16px rgba(0, 0, 0, 0.4);
            --shadow-lg: 0 8px 32px rgba(0, 0, 0, 0.5);
            --shadow-glow: 0 0 20px rgba(59, 130, 246, 0.3);
        }
        
        body {
            font-family: 'Inter', 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: var(--bg-primary);
            background-image: 
                radial-gradient(circle at 20% 50%, rgba(59, 130, 246, 0.05) 0%, transparent 50%),
                radial-gradient(circle at 80% 80%, rgba(16, 185, 129, 0.05) 0%, transparent 50%),
                linear-gradient(rgba(59, 130, 246, 0.02) 1px, transparent 1px),
                linear-gradient(90deg, rgba(59, 130, 246, 0.02) 1px, transparent 1px);
            background-size: 100% 100%, 100% 100%, 50px 50px, 50px 50px;
            color: var(--text-primary);
            min-height: 100vh;
            padding: 20px;
        }
        
        .container { 
            max-width: 1800px; 
            margin: 0 auto;
        }
        
        /* Header Styles */
        .header-bar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding: 20px 30px;
            background: linear-gradient(135deg, var(--bg-card) 0%, var(--bg-secondary) 100%);
            border: 1px solid var(--border-primary);
            border-radius: 16px;
            box-shadow: var(--shadow-md);
            backdrop-filter: blur(10px);
        }
        
        .user-info {
            display: flex;
            align-items: center;
            gap: 12px;
            color: var(--text-secondary);
            font-size: 0.95em;
            font-weight: 500;
        }
        
        .user-info strong {
            color: var(--accent-blue);
            font-weight: 600;
        }
        
        .user-avatar {
            width: 36px;
            height: 36px;
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 18px;
        }
        
        .header-buttons {
            display: flex;
            gap: 12px;
        }
        
        .btn-add-model, .logout-btn, .btn-change-sequence {
            padding: 12px 24px;
            border: none;
            border-radius: 10px;
            color: white;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 0.5px;
            display: flex;
            align-items: center;
            gap: 8px;
            box-shadow: var(--shadow-sm);
            text-decoration: none;
        }
        
        .btn-change-sequence {
            background: linear-gradient(135deg, var(--accent-purple), #7c3aed);
        }
        
        .btn-change-sequence:hover {
            background: linear-gradient(135deg, #7c3aed, #6d28d9);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(139, 92, 246, 0.4);
        }
        
        .btn-add-model {
            background: linear-gradient(135deg, var(--accent-green), #059669);
        }
        
        .btn-add-model:hover {
            background: linear-gradient(135deg, #059669, #047857);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(16, 185, 129, 0.4);
        }
        
        .logout-btn {
            background: linear-gradient(135deg, var(--accent-red), #dc2626);
        }
        
        .logout-btn:hover {
            background: linear-gradient(135deg, #dc2626, #b91c1c);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(239, 68, 68, 0.4);
        }
        
        /* Title */
        h1 {
            text-align: center;
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
            margin-bottom: 30px;
            font-size: 2.8em;
            font-weight: 800;
            letter-spacing: -0.5px;
            position: relative;
            padding-bottom: 20px;
        }
        
        h1::after {
            content: '';
            position: absolute;
            bottom: 0;
            left: 50%;
            transform: translateX(-50%);
            width: 200px;
            height: 4px;
            background: linear-gradient(90deg, transparent, var(--accent-blue), transparent);
            border-radius: 2px;
        }
        
        /* Sequence Info */
        .sequence-info {
            background: linear-gradient(135deg, rgba(251, 191, 36, 0.1), rgba(245, 158, 11, 0.05));
            border: 2px solid rgba(251, 191, 36, 0.3);
            border-radius: 16px;
            padding: 24px;
            margin-bottom: 30px;
            text-align: center;
            position: relative;
            overflow: hidden;
            box-shadow: var(--shadow-md);
        }
        
        .sequence-info::before {
            content: '';
            position: absolute;
            top: -50%;
            left: -50%;
            width: 200%;
            height: 200%;
            background: linear-gradient(45deg, transparent, rgba(251, 191, 36, 0.1), transparent);
            animation: shimmer 3s infinite;
        }
        
        @keyframes shimmer {
            0% { transform: translateX(-100%) translateY(-100%) rotate(45deg); }
            100% { transform: translateX(100%) translateY(100%) rotate(45deg); }
        }
        
        .sequence-info h3 {
            color: var(--accent-yellow);
            font-size: 1.4em;
            margin-bottom: 12px;
            font-weight: 700;
            letter-spacing: 1px;
            position: relative;
            z-index: 1;
        }
        
        .sequence-current {
            font-size: 2.2em;
            color: var(--accent-orange);
            font-weight: 800;
            margin-bottom: 8px;
            position: relative;
            z-index: 1;
            text-shadow: 0 0 20px rgba(245, 158, 11, 0.5);
        }
        
        .sequence-description {
            color: var(--text-secondary);
            font-size: 0.95em;
            position: relative;
            z-index: 1;
            font-weight: 500;
        }
        
        /* Summary Cards */
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        
        .summary-card {
            background: linear-gradient(135deg, var(--bg-card), var(--bg-secondary));
            border: 1px solid var(--border-primary);
            border-radius: 16px;
            padding: 24px;
            text-align: center;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: var(--shadow-sm);
            position: relative;
            overflow: hidden;
        }
        
        .summary-card::before {
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            height: 4px;
            background: linear-gradient(90deg, var(--accent-blue), var(--accent-purple));
            transform: scaleX(0);
            transition: transform 0.3s;
        }
        
        .summary-card:hover {
            border-color: var(--accent-blue);
            transform: translateY(-5px);
            box-shadow: var(--shadow-lg), var(--shadow-glow);
        }
        
        .summary-card:hover::before {
            transform: scaleX(1);
        }
        
        .summary-card h4 {
            color: var(--text-secondary);
            font-size: 0.85em;
            text-transform: uppercase;
            margin-bottom: 12px;
            letter-spacing: 1.5px;
            font-weight: 600;
        }
        
        .summary-card .value {
            font-size: 2.5em;
            font-weight: 800;
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        .summary-card.error .value {
            background: linear-gradient(135deg, var(--accent-red), #dc2626);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            background-clip: text;
        }
        
        /* Model Sections */
        .model-sections {
            display: flex;
            flex-direction: column;
            gap: 40px;
        }
        
        .model-group {
            background: linear-gradient(135deg, var(--bg-card), var(--bg-secondary));
            border: 1px solid var(--border-primary);
            border-radius: 20px;
            padding: 30px;
            box-shadow: var(--shadow-lg);
        }
        
        .model-group-title {
            font-size: 1.8em;
            margin-bottom: 24px;
            padding-bottom: 16px;
            border-bottom: 2px solid var(--border-primary);
            font-weight: 700;
            letter-spacing: 0.5px;
            display: flex;
            align-items: center;
            gap: 12px;
        }
        
        .model-group-title.ford {
            color: var(--accent-blue);
        }
        
        .model-group-title.sona {
            color: var(--accent-green);
        }
        
        .type-badge {
            padding: 6px 16px;
            border-radius: 20px;
            font-size: 0.5em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .ford-badge {
            background: linear-gradient(135deg, var(--accent-blue), #2563eb);
            box-shadow: 0 2px 10px rgba(59, 130, 246, 0.3);
        }
        
        .sona-badge {
            background: linear-gradient(135deg, var(--accent-green), #059669);
            box-shadow: 0 2px 10px rgba(16, 185, 129, 0.3);
        }
        
        /* Stats Grid */
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
            gap: 24px;
        }
        
        .stat-card {
            background: linear-gradient(135deg, rgba(26, 31, 46, 0.8), rgba(21, 26, 39, 0.8));
            border: 1px solid var(--border-primary);
            border-radius: 16px;
            padding: 24px;
            position: relative;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            box-shadow: var(--shadow-sm);
            backdrop-filter: blur(10px);
        }
        
        .stat-card:hover {
            transform: translateY(-5px);
            border-color: var(--accent-blue);
            box-shadow: var(--shadow-lg), var(--shadow-glow);
        }
        
        .stat-card.active-sequence {
            border: 2px solid var(--accent-orange);
            box-shadow: 0 0 30px rgba(245, 158, 11, 0.3);
        }
        
        .action-buttons {
            position: absolute;
            top: 16px;
            right: 16px;
            display: flex;
            gap: 8px;
            z-index: 10;
        }
        
        .btn-edit-model, .btn-delete-model {
            padding: 8px 14px;
            border: none;
            border-radius: 8px;
            color: white;
            font-weight: 600;
            cursor: pointer;
            font-size: 0.75em;
            transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
            display: flex;
            align-items: center;
            gap: 4px;
            box-shadow: var(--shadow-sm);
        }
        
        .btn-edit-model {
            background: linear-gradient(135deg, var(--accent-blue), #2563eb);
        }
        
        .btn-edit-model:hover {
            background: linear-gradient(135deg, #2563eb, #1d4ed8);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(59, 130, 246, 0.4);
        }
        
        .btn-delete-model {
            background: linear-gradient(135deg, var(--accent-red), #dc2626);
        }
        
        .btn-delete-model:hover {
            background: linear-gradient(135deg, #dc2626, #b91c1c);
            transform: translateY(-2px);
            box-shadow: 0 4px 15px rgba(239, 68, 68, 0.4);
        }
        
        .stat-card h3 {
            color: var(--accent-blue);
            margin-bottom: 20px;
            font-size: 1.3em;
            padding-bottom: 12px;
            border-bottom: 2px solid rgba(59, 130, 246, 0.3);
            font-weight: 700;
            display: flex;
            align-items: center;
            gap: 8px;
            padding-right: 100px;
        }
        
        .sequence-badge, .current-sequence {
            padding: 4px 10px;
            border-radius: 12px;
            font-size: 0.65em;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .sequence-badge {
            background: linear-gradient(135deg, var(--accent-purple), #7c3aed);
            box-shadow: 0 2px 8px rgba(139, 92, 246, 0.3);
        }
        
        .current-sequence {
            background: linear-gradient(135deg, var(--accent-orange), #d97706);
            animation: pulse-glow 2s infinite;
            box-shadow: 0 0 20px rgba(245, 158, 11, 0.5);
        }
        
        @keyframes pulse-glow {
            0%, 100% { 
                box-shadow: 0 0 20px rgba(245, 158, 11, 0.5);
                transform: scale(1);
            }
            50% { 
                box-shadow: 0 0 30px rgba(245, 158, 11, 0.8);
                transform: scale(1.05);
            }
        }
        
        .stat-row {
            display: flex;
            justify-content: space-between;
            padding: 12px 0;
            border-bottom: 1px solid rgba(45, 55, 72, 0.5);
            align-items: center;
        }
        
        .stat-row:last-child { 
            border-bottom: none; 
        }
        
        .stat-label {
            font-weight: 600;
            color: var(--text-secondary);
            font-size: 0.9em;
            letter-spacing: 0.3px;
        }
        
        .stat-value {
            font-weight: 700;
            color: var(--text-primary);
            font-size: 1.05em;
        }
        
        /* Status Indicators */
        .status-idle { 
            color: var(--accent-green);
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .status-processing { 
            color: var(--accent-yellow);
            display: flex;
            align-items: center;
            gap: 8px;
            animation: pulse-text 1.5s infinite;
        }
        
        @keyframes pulse-text {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.6; }
        }
        
        .indicator {
            display: inline-block;
            width: 10px;
            height: 10px;
            border-radius: 50%;
            animation: blink 2s infinite;
        }
        
        .indicator-idle { 
            background: var(--accent-green);
            box-shadow: 0 0 10px var(--accent-green);
        }
        
        .indicator-processing { 
            background: var(--accent-yellow);
            box-shadow: 0 0 10px var(--accent-yellow);
        }
        
        @keyframes blink {
            0%, 100% { opacity: 1; transform: scale(1); }
            50% { opacity: 0.4; transform: scale(0.8); }
        }
        
        /* Logs Section */
        .logs-section {
            background: linear-gradient(135deg, var(--bg-card), var(--bg-secondary));
            border: 1px solid var(--border-primary);
            border-radius: 20px;
            padding: 30px;
            box-shadow: var(--shadow-lg);
            max-height: 600px;
            overflow-y: auto;
            margin-top: 40px;
        }
        
        .logs-section::-webkit-scrollbar { width: 12px; }
        .logs-section::-webkit-scrollbar-track { 
            background: var(--bg-secondary); 
            border-radius: 10px; 
        }
        .logs-section::-webkit-scrollbar-thumb { 
            background: linear-gradient(135deg, var(--accent-blue), var(--accent-purple));
            border-radius: 10px; 
            border: 2px solid var(--bg-secondary);
        }
        .logs-section::-webkit-scrollbar-thumb:hover { 
            background: linear-gradient(135deg, var(--accent-purple), var(--accent-blue));
        }
        
        .logs-section h2 {
            color: var(--accent-blue);
            margin-bottom: 24px;
            font-size: 1.6em;
            padding-bottom: 16px;
            border-bottom: 2px solid var(--border-primary);
            font-weight: 700;
            letter-spacing: 0.5px;
        }
        
        .log-entry {
            padding: 12px 18px;
            margin: 10px 0;
            border-radius: 10px;
            font-size: 0.9em;
            font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
            border-left: 4px solid;
            transition: all 0.2s;
            line-height: 1.6;
        }
        
        .log-entry:hover {
            transform: translateX(4px);
        }
        
        .log-INFO { 
            background: rgba(16, 185, 129, 0.08);
            border-left-color: var(--accent-green);
            color: #6ee7b7;
        }
        
        .log-WARNING { 
            background: rgba(251, 191, 36, 0.08);
            border-left-color: var(--accent-yellow);
            color: #fcd34d;
        }
        
        .log-ERROR { 
            background: rgba(239, 68, 68, 0.08);
            border-left-color: var(--accent-red);
            color: #fca5a5;
        }
        
        .timestamp {
            color: var(--text-muted);
            font-size: 0.85em;
            margin-right: 12px;
            font-weight: 600;
        }
        
        /* Update Time */
        .update-time {
            text-align: center;
            color: var(--accent-blue);
            margin-top: 30px;
            padding: 20px;
            background: rgba(59, 130, 246, 0.05);
            border-radius: 12px;
            border: 1px solid rgba(59, 130, 246, 0.2);
            font-size: 0.95em;
            font-weight: 600;
            display: flex;
            align-items: center;
            justify-content: center;
            gap: 10px;
        }
        
        .pulse-dot {
            width: 8px;
            height: 8px;
            background: var(--accent-green);
            border-radius: 50%;
            animation: pulse-dot 2s infinite;
        }
        
        @keyframes pulse-dot {
            0%, 100% { 
                box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7);
            }
            50% { 
                box-shadow: 0 0 0 8px rgba(16, 185, 129, 0);
            }
        }

        /* ========== SEQUENCE EDITOR MODAL STYLES ========== */
        
        .sequence-editor-modal {
            display: none;
            position: fixed;
            z-index: 2000;
            left: 0;
            top: 0;
            width: 100%;
            height: 100%;
            background-color: rgba(0,0,0,0.85);
            animation: fadeIn 0.3s;
            backdrop-filter: blur(8px);
        }
        
        .sequence-editor-content {
            background: linear-gradient(135deg, var(--bg-card), var(--bg-secondary));
            border: 2px solid var(--accent-purple);
            margin: 3% auto;
            padding: 40px;
            width: 90%;
            max-width: 900px;
            border-radius: 20px;
            box-shadow: var(--shadow-lg), 0 0 40px rgba(139, 92, 246, 0.4);
            max-height: 85vh;
            overflow-y: auto;
        }
        
        .sequence-editor-content::-webkit-scrollbar { width: 12px; }
        .sequence-editor-content::-webkit-scrollbar-track { 
            background: var(--bg-secondary); 
            border-radius: 10px; 
        }
        .sequence-editor-content::-webkit-scrollbar-thumb { 
            background: linear-gradient(135deg, var(--accent-purple), #7c3aed);
            border-radius: 10px;
        }
        
        .sequence-editor-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 30px;
            padding-bottom: 20px;
            border-bottom: 2px solid var(--border-primary);
        }
        
        .sequence-editor-header h2 {
            color: var(--accent-purple);
            font-size: 2em;
            font-weight: 800;
            display: flex;
            align-items: center;
            gap: 12px;
            margin: 0;
        }
        
        .close-sequence-editor {
            background: linear-gradient(135deg, var(--accent-red), #dc2626);
            border: none;
            padding: 12px 20px;
            border-radius: 10px;
            color: white;
            font-weight: 700;
            cursor: pointer;
            font-size: 1.1em;
            transition: all 0.3s;
            box-shadow: var(--shadow-sm);
        }
        
        .close-sequence-editor:hover {
            background: linear-gradient(135deg, #dc2626, #b91c1c);
            transform: translateY(-2px);
            box-shadow: 0 6px 20px rgba(239, 68, 68, 0.5);
        }
        
        .sequence-info-box {
            background: rgba(139, 92, 246, 0.1);
            border: 2px solid rgba(139, 92, 246, 0.3);
            border-radius: 12px;
            padding: 20px;
            margin-bottom: 30px;
        }
        
        .sequence-info-box h3 {
            color: var(--accent-purple);
            margin-bottom: 12px;
            font-size: 1.2em;
        }
        
        .sequence-info-box ul {
            margin-left: 20px;
            color: var(--text-secondary);
            line-height: 1.8;
        }
        
        .sequence-info-box strong {
            color: var(--accent-yellow);
        }
        
        .sequence-models-grid {
            display: grid;
            gap: 20px;
        }
        
        .sequence-model-row {
            background: linear-gradient(135deg, rgba(26, 31, 46, 0.8), rgba(21, 26, 39, 0.8));
            border: 2px solid var(--border-primary);
            border-radius: 12px;
            padding: 20px;
            display: grid;
            grid-template-columns: 1fr 200px 150px;
            gap: 20px;
            align-items: center;
            transition: all 0.3s;
        }
        
        .sequence-model-row:hover {
            border-color: var(--accent-purple);
            transform: translateX(5px);
            box-shadow: 0 4px 20px rgba(139, 92, 246, 0.2);
        }
        
        .sequence-model-row.disabled {
            opacity: 0.6;
            border-color: #6b7280;
        }
        
        .model-info {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .model-name-display {
            font-size: 1.3em;
            font-weight: 700;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .model-type-badge {
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 0.5em;
            font-weight: 600;
            text-transform: uppercase;
        }
        
        .model-type-badge.ford {
            background: linear-gradient(135deg, var(--accent-blue), #2563eb);
        }
        
        .model-type-badge.sona {
            background: linear-gradient(135deg, var(--accent-green), #059669);
        }
        
        .model-details {
            color: var(--text-muted);
            font-size: 0.9em;
        }
        
        .sequence-input-group {
            display: flex;
            flex-direction: column;
            gap: 8px;
        }
        
        .sequence-input-label {
            color: var(--text-secondary);
            font-size: 0.85em;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .sequence-input {
            width: 100%;
            padding: 12px 16px;
            background: var(--bg-primary);
            border: 2px solid var(--border-primary);
            border-radius: 10px;
            color: var(--text-primary);
            font-size: 1.2em;
            font-weight: 700;
            text-align: center;
            transition: all 0.3s;
        }
        
        .sequence-input:focus {
            outline: none;
            border-color: var(--accent-purple);
            box-shadow: 0 0 20px rgba(139, 92, 246, 0.3);
        }
        
        .sequence-input.disabled {
            background: #1f2937;
            color: #6b7280;
        }
        
        .sequence-status {
            display: flex;
            flex-direction: column;
            align-items: center;
            gap: 8px;
        }
        
        .status-indicator {
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 700;
            font-size: 0.85em;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }
        
        .status-active {
            background: linear-gradient(135deg, var(--accent-green), #059669);
            box-shadow: 0 0 20px rgba(16, 185, 129, 0.4);
            animation: pulse-glow 2s infinite;
        }
        
        .status-disabled {
            background: linear-gradient(135deg, #6b7280, #4b5563);
        }
        
        .btn-save-sequences {
            width: 100%;
            padding: 18px;
            background: linear-gradient(135deg, var(--accent-purple), #7c3aed);
            border: none;
            border-radius: 12px;
            color: white;
            font-size: 1.2em;
            font-weight: 700;
            text-transform: uppercase;
            letter-spacing: 1px;
            cursor: pointer;
            margin-top: 30px;
            transition: all 0.3s;
            box-shadow: var(--shadow-md);
        }
        
        .btn-save-sequences:hover {
            background: linear-gradient(135deg, #7c3aed, #6d28d9);
            transform: translateY(-3px);
            box-shadow: 0 8px 30px rgba(139, 92, 246, 0.5);
        }
        
        .btn-save-sequences:active {
            transform: translateY(-1px);
        }
        
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        
        /* Responsive Design */
        @media (max-width: 1200px) {
            .stats-grid {
                grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
            }
        }
        
        @media (max-width: 768px) {
            h1 { font-size: 2em; }
            .summary-cards {
                grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
            }
            .stats-grid {
                grid-template-columns: 1fr;
            }
            .header-bar {
                flex-direction: column;
                gap: 20px;
            }
            .sequence-model-row {
                grid-template-columns: 1fr;
            }
            .sequence-editor-content {
                padding: 20px;
                margin: 2% 10px;
            }
        }
    </style>
</head>
<body>
    <div class="container">
        <!-- Header Bar -->
        <div class="header-bar">
            <div class="user-info">
                <div class="user-avatar">👤</div>
                <span>Logged in as: <strong>{{ username }}</strong></span>
            </div>
            <div class="header-buttons">
                {% if is_admin %}
                <button class="btn-change-sequence" id="btnChangeSequence">
                    <span>🔢</span> Change Sequence
                </button>
                {% endif %}
                <a href="{{ url_for('logout') }}" class="logout-btn">
                    <span>🚪</span> Logout
                </a>
            </div>
        </div>
        
        <!-- Main Title -->
        <h1>🔄 Sequential CSV Processor</h1>
        
        <!-- Real-time Sequence Info -->
        <div class="sequence-info">
            <h3>⚡ LIVE PROCESSING STATUS</h3>
            <div class="sequence-current" id="currentSequence">Sequence #{{ current_sequence }}</div>
            <div class="sequence-description">
                Processing one file per model in priority order | 30s timeout per file
            </div>
        </div>
        
        <!-- Summary Cards -->
        <div class="summary-cards">
            <div class="summary-card">
                <h4>📊 Total Models</h4>
                <div class="value">{{ stats|length }}</div>
            </div>
            <div class="summary-card">
                <h4>✅ Files Processed</h4>
                <div class="value">{{ total_files }}</div>
            </div>
            <div class="summary-card error">
                <h4>❌ Failed Files</h4>
                <div class="value">{{ total_failed }}</div>
            </div>
            <div class="summary-card">
                <h4>⚡ Success Rate</h4>
                <div class="value">{{ "%.1f"|format((total_files / (total_files + total_failed) * 100) if (total_files + total_failed) > 0 else 0) }}%</div>
            </div>
        </div>
        
        <!-- Model Sections -->
        <div class="model-sections">
            <!-- FORD Models -->
            {% set ford_models = [] %}
            {% for name, stat in stats.items() %}
                {% if stat.type == 'ford' %}
                    {% set _ = ford_models.append((name, stat)) %}
                {% endif %}
            {% endfor %}
            
            {% if ford_models|length > 0 %}
            <div class="model-group">
                <h2 class="model-group-title ford">
                    🚗 FORD MODELS
                    <span class="type-badge ford-badge">Row-Based Processing</span>
                </h2>
                <div class="stats-grid">
                    {% for name, stat in ford_models %}
                    <div class="stat-card {% if stat.sequence_number == current_sequence %}active-sequence{% endif %}" data-sequence="{{ stat.sequence_number }}">
                      {# Temporarily hidden
                        {% if is_admin %}
                        <div class="action-buttons">
                            <button class="btn-edit-model" onclick="editModel('{{ name }}')">✏️</button>
                            <button class="btn-delete-model" onclick="deleteModel('{{ name }}')">🗑️</button>
                        </div>
                        {% endif %}
                        #}
                        <h3>
                            🔧 {{ name }}
                            <span class="sequence-badge-container">
                                {% if stat.sequence_number == current_sequence %}
                                <span class="current-sequence">ACTIVE #{{ stat.sequence_number }}</span>
                                {% elif stat.sequence_number == 0 %}
                                <span class="sequence-badge" style="background: linear-gradient(135deg, #6b7280, #4b5563); box-shadow: none;">⏸️ DISABLED</span>
                                {% else %}
                                <span class="sequence-badge">Seq #{{ stat.sequence_number }}</span>
                                {% endif %}
                            </span>
                        </h3>
                        <div class="stat-row">
                            <span class="stat-label">Status</span>
                            <span class="stat-value status-{{ 'idle' if stat.status == 'Idle' else 'processing' }}">
                                <span class="indicator indicator-{{ 'idle' if stat.status == 'Idle' else 'processing' }}"></span>
                                {{ stat.status }}
                            </span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Files Processed</span>
                            <span class="stat-value">{{ stat.total_files }}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Total Rows</span>
                            <span class="stat-value">{{ "{:,}".format(stat.total_rows) }}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Failed Files</span>
                            <span class="stat-value" style="color: var(--accent-red);">{{ stat.failed_files }}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Avg Time</span>
                            <span class="stat-value">{{ "%.2f"|format(stat.avg_process_time) }}s</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Last File</span>
                            <span class="stat-value" style="font-size: 0.75em; word-break: break-all; color: var(--text-secondary);">
                                {{ stat.last_file or 'None' }}
                            </span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Last Processed</span>
                            <span class="stat-value" style="font-size: 0.8em; color: var(--text-secondary);">{{ stat.last_processed or 'N/A' }}</span>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endif %}
            
            <!-- SONA Models -->
            {% set sona_models = [] %}
            {% for name, stat in stats.items() %}
                {% if stat.type == 'sona' %}
                    {% set _ = sona_models.append((name, stat)) %}
                {% endif %}
            {% endfor %}
            
            {% if sona_models|length > 0 %}
            <div class="model-group">
                <h2 class="model-group-title sona">
                    🏭 SONA MODELS
                    <span class="type-badge sona-badge">Cell-Based Processing</span>
                </h2>
                <div class="stats-grid">
                    {% for name, stat in sona_models %}
                    <div class="stat-card {% if stat.sequence_number == current_sequence %}active-sequence{% endif %}" data-sequence="{{ stat.sequence_number }}">
                        {# Temporarily hidden
                            {% if is_admin %}
                            <div class="action-buttons">
                                <button class="btn-edit-model" onclick="editModel('{{ name }}')">✏️</button>
                                <button class="btn-delete-model" onclick="deleteModel('{{ name }}')">🗑️</button>
                            </div>
                            {% endif %}
                            #}
                        <h3>
                            🔧 {{ name }}
                            <span class="sequence-badge-container">
                                {% if stat.sequence_number == current_sequence %}
                                <span class="current-sequence">ACTIVE #{{ stat.sequence_number }}</span>
                                {% elif stat.sequence_number == 0 %}
                                <span class="sequence-badge" style="background: linear-gradient(135deg, #6b7280, #4b5563); box-shadow: none;">⏸️ DISABLED</span>
                                {% else %}
                                <span class="sequence-badge">Seq #{{ stat.sequence_number }}</span>
                                {% endif %}
                            </span>
                        </h3>
                        <div class="stat-row">
                            <span class="stat-label">Status</span>
                            <span class="stat-value status-{{ 'idle' if stat.status == 'Idle' else 'processing' }}">
                                <span class="indicator indicator-{{ 'idle' if stat.status == 'Idle' else 'processing' }}"></span>
                                {{ stat.status }}
                            </span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Files Processed</span>
                            <span class="stat-value">{{ stat.total_files }}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Failed Files</span>
                            <span class="stat-value" style="color: var(--accent-red);">{{ stat.failed_files }}</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Avg Time</span>
                            <span class="stat-value">{{ "%.2f"|format(stat.avg_process_time) }}s</span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Last File</span>
                            <span class="stat-value" style="font-size: 0.75em; word-break: break-all; color: var(--text-secondary);">
                                {{ stat.last_file or 'None' }}
                            </span>
                        </div>
                        <div class="stat-row">
                            <span class="stat-label">Last Processed</span>
                            <span class="stat-value" style="font-size: 0.8em; color: var(--text-secondary);">{{ stat.last_processed or 'N/A' }}</span>
                        </div>
                    </div>
                    {% endfor %}
                </div>
            </div>
            {% endif %}
        </div>
        
        <!-- Logs Section -->
        <div class="logs-section">
            <h2>📋 Recent Activity Logs</h2>
            <div id="logsContainer">
                {% for log in logs %}
                <div class="log-entry log-{{ log.level }}">
                    <span class="timestamp">{{ log.timestamp }}</span>
                    <strong>[{{ log.level }}]</strong> {{ log.message }}
                </div>
                {% endfor %}
            </div>
        </div>
        
        <!-- Update Time -->
        <div class="update-time">
            <span class="pulse-dot"></span>
            <span>Live updates every 3 seconds</span>
            <span>•</span>
            <span>Last update: <strong id="lastUpdateTime">{{ now }}</strong></span>
        </div>
    </div>

    <!-- Sequence Editor Modal -->
    <div id="sequenceEditorModal" class="sequence-editor-modal">
        <div class="sequence-editor-content">
            <div class="sequence-editor-header">
                <h2>🔢 Edit Processing Sequence</h2>
                <button class="close-sequence-editor" id="btnCloseSequence">✕ Close</button>
            </div>
            
            <div class="sequence-info-box">
                <h3>💡 How Sequence Numbers Work:</h3>
                <ul>
                    <li><strong>Lower numbers = Higher priority</strong> (Sequence 1 processes before Sequence 2)</li>
                    <li><strong>Set to 0</strong> to completely disable a model (it won't process any files)</li>
                    <li>The system processes <strong>one file per model</strong> in sequence order</li>
                    <li>Changes take effect <strong>immediately</strong> after saving</li>
                </ul>
            </div>
            
            <form id="sequenceForm">
                <div class="sequence-models-grid" id="sequenceModelsGrid">
                    <!-- Models will be populated here by JavaScript -->
                </div>
                
                <button type="submit" class="btn-save-sequences">
                    💾 Save All Changes
                </button>
            </form>
        </div>
    </div>

    <script>
    let selectedType = null;
    let modalOpen = false;
    let autoRefreshInterval = null;

    // Auto-update function using AJAX
    function updateDashboard() {
        if (modalOpen) return;
        
        fetch('/api/stats')
            .then(response => response.json())
            .then(data => {
                const currentSeq = data.current_sequence;
                document.getElementById('currentSequence').textContent = `Sequence #${currentSeq}`;
                
                document.querySelectorAll('.stat-card').forEach(card => {
                    const modelSeq = parseInt(card.dataset.sequence);
                    const sequenceBadgeContainer = card.querySelector('.sequence-badge-container');
                    
                    if (sequenceBadgeContainer) {
                        if (modelSeq === currentSeq) {
                            card.classList.add('active-sequence');
                            sequenceBadgeContainer.innerHTML = `<span class="current-sequence">ACTIVE #${modelSeq}</span>`;
                        } else if (modelSeq === 0) {
                            card.classList.remove('active-sequence');
                            sequenceBadgeContainer.innerHTML = `<span class="sequence-badge" style="background: linear-gradient(135deg, #6b7280, #4b5563); box-shadow: none;">⏸️ DISABLED</span>`;
                        } else {
                            card.classList.remove('active-sequence');
                            sequenceBadgeContainer.innerHTML = `<span class="sequence-badge">Seq #${modelSeq}</span>`;
                        }
                    }
                });
                
                const now = new Date();
                const timestamp = now.toLocaleString('en-GB', {
                    year: 'numeric',
                    month: '2-digit',
                    day: '2-digit',
                    hour: '2-digit',
                    minute: '2-digit',
                    second: '2-digit',
                    hour12: false
                }).replace(',', '');
                document.getElementById('lastUpdateTime').textContent = timestamp;
            })
            .catch(error => {
                console.error('Error updating dashboard:', error);
            });
    }

    function startAutoRefresh() {
        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval);
        }
        
        autoRefreshInterval = setInterval(updateDashboard, 3000);
        
        setInterval(function() {
            if (!modalOpen) {
                window.location.reload();
            }
        }, 30000);
    }

    window.onload = function() {
        startAutoRefresh();
    };

    // ===== SEQUENCE EDITOR FUNCTIONS =====
    
 function openSequenceEditor() {
        console.log('Opening sequence editor...'); // Debug log
        modalOpen = true;
        
        fetch('/api/stats')
            .then(response => response.json())
            .then(data => {
                const stats = data.stats;
                const container = document.getElementById('sequenceModelsGrid');
                container.innerHTML = '';
                
                const sortedModels = Object.entries(stats).sort((a, b) => {
                    const seqA = a[1].sequence_number || 999;
                    const seqB = b[1].sequence_number || 999;
                    return seqA - seqB;
                });
                
                sortedModels.forEach(([modelName, stat]) => {
                    const modelRow = document.createElement('div');
                    modelRow.className = 'sequence-model-row';
                    if (stat.sequence_number === 0) {
                        modelRow.classList.add('disabled');
                    }
                    
                    const typeClass = stat.type === 'ford' ? 'ford' : 'sona';
                    const typeIcon = stat.type === 'ford' ? '🚗' : '🏭';
                    
                    modelRow.innerHTML = `
                        <div class="model-info">
                            <div class="model-name-display">
                                ${typeIcon} ${modelName}
                                <span class="model-type-badge ${typeClass}">${stat.type.toUpperCase()}</span>
                            </div>
                            <div class="model-details">
                                Files: ${stat.total_files} | Failed: ${stat.failed_files} | Avg: ${stat.avg_process_time.toFixed(2)}s
                            </div>
                        </div>
                        <div class="sequence-input-group">
                            <label class="sequence-input-label">Sequence Number</label>
                            <input type="number" 
                                   class="sequence-input ${stat.sequence_number === 0 ? 'disabled' : ''}" 
                                   name="${modelName}" 
                                   value="${stat.sequence_number}" 
                                   min="0" 
                                   max="999"
                                   data-original="${stat.sequence_number}">
                        </div>
                        <div class="sequence-status">
                            <span class="status-indicator ${stat.sequence_number === 0 ? 'status-disabled' : 'status-active'}">
                                ${stat.sequence_number === 0 ? '⏸️ DISABLED' : '✓ ACTIVE'}
                            </span>
                        </div>
                    `;
                    
                    const input = modelRow.querySelector('input');
                    const statusIndicator = modelRow.querySelector('.status-indicator');
                    
                    input.addEventListener('input', function() {
                        const value = parseInt(this.value) || 0;
                        if (value === 0) {
                            statusIndicator.textContent = '⏸️ DISABLED';
                            statusIndicator.className = 'status-indicator status-disabled';
                            this.classList.add('disabled');
                            modelRow.classList.add('disabled');
                        } else {
                            statusIndicator.textContent = '✓ ACTIVE';
                            statusIndicator.className = 'status-indicator status-active';
                            this.classList.remove('disabled');
                            modelRow.classList.remove('disabled');
                        }
                    });
                    
                    container.appendChild(modelRow);
                });
                
                document.getElementById('sequenceEditorModal').style.display = 'block';
                // Attach close button listener
                const btnClose = document.getElementById('btnCloseSequence');
                if (btnClose) {
                    btnClose.onclick = function() {
                        closeSequenceEditor();
                    };
                }
                console.log('Sequence editor opened'); // Debug log
            })
            .catch(error => {
                console.error('Error loading sequence editor:', error);
                alert('Error loading models: ' + error.message);
                modalOpen = false;
            });
    }
    
    function closeSequenceEditor() {
        console.log('Closing sequence editor...'); // Debug log
        modalOpen = false;
        document.getElementById('sequenceEditorModal').style.display = 'none';
    }
    
    // Initialize on page load
    window.onload = function() {
        console.log('Page loaded, initializing...'); // Debug log
        startAutoRefresh();
            // Add event listener for Change Sequence button
            const btnChangeSequence = document.getElementById('btnChangeSequence');
            if (btnChangeSequence) {
                btnChangeSequence.addEventListener('click', function(e) {
                    e.preventDefault();
                    console.log('Change Sequence button clicked!'); // Debug
                    openSequenceEditor();
                });
                console.log('Change Sequence button listener attached'); // Debug
            } else {
                console.error('Change Sequence button not found!'); // Debug
            }
        
        // Setup form submission
        const form = document.getElementById('sequenceForm');
        if (form) {
            form.addEventListener('submit', function(e) {
                e.preventDefault();
                
                const inputs = document.querySelectorAll('.sequence-input');
                const updates = [];
                let hasChanges = false;
                
                inputs.forEach(input => {
                    const modelName = input.name;
                    const newSeq = parseInt(input.value) || 0;
                    const originalSeq = parseInt(input.dataset.original);
                    
                    if (newSeq !== originalSeq) {
                        hasChanges = true;
                    }
                    
                    updates.push({
                        model_name: modelName,
                        sequence_number: newSeq
                    });
                });
                
                if (!hasChanges) {
                    alert('No changes detected.');
                    return;
                }
                
                if (!confirm(`Save sequence changes for ${updates.length} model(s)?`)) {
                    return;
                }
                
                fetch('/api/update_sequences_bulk', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({ updates: updates })
                })
                .then(response => response.json())
                .then(data => {
                    if (data.success) {
                        alert('✅ ' + data.message);
                        closeSequenceEditor();
                        window.location.reload();
                    } else {
                        alert('❌ Error: ' + data.error);
                    }
                })
                .catch(error => {
                    alert('❌ Error: ' + error.message);
                })
                .finally(() => {
                    modalOpen = false;
                });
            });
        }
    };
    
    // Close modal when clicking outside
    window.onclick = function(event) {
        const modal = document.getElementById('sequenceEditorModal');
        if (event.target === modal) {
            closeSequenceEditor();
        }
    };
    
    </script>
</body>
</html>
"""
# ========================= FLASK ROUTES =========================
@app.route('/login', methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if username in USERS and check_password_hash(USERS[username], password):
            session['logged_in'] = True
            session['username'] = username
            logging.info(f"✅ User '{username}' logged in successfully")
            return redirect(url_for('dashboard'))
        else:
            logging.warning(f"⚠️ Failed login attempt for username: {username}")
            error = 'Invalid username or password'
    
    return render_template_string(LOGIN_TEMPLATE, error=error)


@app.route('/logout')
def logout():
    username = session.get('username', 'Unknown')
    session.pop('logged_in', None)
    session.pop('username', None)
    logging.info(f"👋 User '{username}' logged out")
    return redirect(url_for('login'))


@app.route('/')
@login_required
def dashboard():
    try:
        stats_copy = global_stats.get_snapshot()
        logs_copy = global_logs.get_snapshot()
    except Exception as e:
        logging.error(f"Dashboard error: {e}")
        stats_copy = {}
        logs_copy = []
    
    total_files = sum(s.get('total_files', 0) for s in stats_copy.values())
    total_rows = sum(s.get('total_rows', 0) for s in stats_copy.values())
    total_failed = sum(s.get('failed_files', 0) for s in stats_copy.values())
    
    # Get current sequence from sequential processor
    current_sequence = sequential_processor.current_sequence
    
    return render_template_string(
        DASHBOARD_TEMPLATE,
        stats=stats_copy,
        logs=reversed(logs_copy),
        now=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        username=session.get('username', 'Unknown'),
        is_admin=(session.get('username') == 'admin'),
        total_files=total_files,
        total_rows=total_rows,
        total_failed=total_failed,
        current_sequence=current_sequence
    )


@app.route('/api/stats')
@login_required
def api_stats():
    return jsonify({
        "stats": global_stats.get_snapshot(),
        "logs": global_logs.get_snapshot(),
        "current_sequence": sequential_processor.current_sequence
    })


@app.route('/api/test_connection', methods=['POST'])
@admin_required
def test_connection_api():
    try:
        data = request.json
        success, message = test_database_connection(
            data.get('db_driver'),
            data.get('db_server'),
            data.get('db_name'),
            data.get('db_username'),
            data.get('db_password')
        )
        return jsonify({"success": success, "message": message})
    except Exception as e:
        return jsonify({"success": False, "message": f"❌ Error: {str(e)}"})


@app.route('/api/get_model')
@admin_required
def get_model():
    model_name = request.args.get('model_name')
    if not model_name:
        return jsonify({"success": False, "error": "Model name required"}), 400
    
    with config_lock:
        if model_name not in UNIFIED_CONFIGS:
            return jsonify({"success": False, "error": "Model not found"}), 404
        
        return jsonify({
            "success": True,
            "config": UNIFIED_CONFIGS[model_name]
        })


@app.route('/api/add_model', methods=['POST'])
@admin_required
def add_model():
    try:
        data = request.json
        model_name = data.get('model_name')
        sequence_number = data.get('sequence_number')
        
        if not model_name or not sequence_number:
            return jsonify({"success": False, "error": "Model name and sequence number are required"})
        
        with config_lock:
            if model_name in UNIFIED_CONFIGS:
                return jsonify({"success": False, "error": "Model already exists"})
            
            # Create new model configuration
            new_config = {
                "type": data.get('type'),
                "watch_folder": data.get('watch_folder'),
                "table_name": data.get('table_name'),
                "db_driver": data.get('db_driver'),
                "db_server": data.get('db_server'),
                "db_name": data.get('db_name'),
                "db_username": data.get('db_username'),
                "db_password": data.get('db_password'),
                "processed_folder": os.path.join(data.get('watch_folder'), 'ReadFiles'),
                "notgood_folder": os.path.join(data.get('watch_folder'), 'NotGoodFiles'),
                "sequence_number": sequence_number
            }
            
            # Add type-specific fields
            if data.get('type') == 'ford':
                new_config.update({
                    "file_prefix": data.get('file_prefix'),
                    "required_columns": data.get('required_columns', [])
                })
            else:  # sona
                new_config.update({
                    "filename_pattern": data.get('filename_pattern'),
                    "status_column_index": data.get('status_column_index', 4),
                    "cell_mapping": data.get('cell_mapping', []),
                    "file_stabilization_delay": 0.5,
                    "data_type_mapping": {
                        "numeric_columns": [],
                        "varchar_columns": []
                    }
                })
            
            UNIFIED_CONFIGS[model_name] = new_config
            save_config(UNIFIED_CONFIGS)
            
            # Initialize stats for new model
            global_stats.initialize(model_name)
            global_stats._stats[model_name]["type"] = data.get('type', 'unknown')
            global_stats._stats[model_name]["sequence_number"] = sequence_number
            
            logging.info(f"✅ Added new model: {model_name} with sequence {sequence_number}")
            
            return jsonify({"success": True, "message": f"Model {model_name} added successfully"})
            
    except Exception as e:
        logging.error(f"Error adding model: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/edit_model', methods=['POST'])
@admin_required
def edit_model():
    try:
        data = request.json
        original_name = data.get('original_model_name')
        model_name = data.get('model_name')
        sequence_number = data.get('sequence_number')
        
        if not original_name or not model_name:
            return jsonify({"success": False, "error": "Model names are required"})
        
        with config_lock:
            if original_name not in UNIFIED_CONFIGS:
                return jsonify({"success": False, "error": "Model not found"})
            
            # Update configuration
            config = UNIFIED_CONFIGS[original_name]
            
            # Update editable fields
            config['sequence_number'] = sequence_number
            config['watch_folder'] = data.get('watch_folder')
            config['db_driver'] = data.get('db_driver')
            config['db_server'] = data.get('db_server')
            config['db_name'] = data.get('db_name')
            config['db_username'] = data.get('db_username')
            config['db_password'] = data.get('db_password')
            
            # Update type-specific editable fields
            if config['type'] == 'ford':
                # Only update non-structural fields for FORD
                pass  # file_prefix and required_columns cannot be changed
            else:  # sona
                config['filename_pattern'] = data.get('filename_pattern')
                config['status_column_index'] = data.get('status_column_index', 4)
                # Update cell positions (but not names)
                if data.get('cell_mapping'):
                    for i, new_mapping in enumerate(data.get('cell_mapping')):
                        if i < len(config['cell_mapping']):
                            config['cell_mapping'][i][0] = new_mapping[0]  # row
                            config['cell_mapping'][i][1] = new_mapping[1]  # column
            
            # If name changed, create new entry and remove old
            if original_name != model_name:
                UNIFIED_CONFIGS[model_name] = config
                del UNIFIED_CONFIGS[original_name]
                
                # Update stats
                if original_name in global_stats._stats:
                    global_stats._stats[model_name] = global_stats._stats[original_name]
                    del global_stats._stats[original_name]
            
            save_config(UNIFIED_CONFIGS)
            
            # Update sequence number in stats
            if model_name in global_stats._stats:
                global_stats._stats[model_name]["sequence_number"] = sequence_number
            
            logging.info(f"✅ Updated model: {model_name} with sequence {sequence_number}")
            
            return jsonify({"success": True, "message": f"Model {model_name} updated successfully"})
            
    except Exception as e:
        logging.error(f"Error editing model: {e}")
        return jsonify({"success": False, "error": str(e)})


@app.route('/api/delete_model', methods=['POST'])
@admin_required  
def delete_model():
    try:
        data = request.json
        model_name = data.get('model_name')
        
        if not model_name:
            return jsonify({"success": False, "error": "Model name required"})
        
        with config_lock:
            if model_name not in UNIFIED_CONFIGS:
                return jsonify({"success": False, "error": "Model not found"})
            
            # Stop watcher if running
            if model_name in observers_dict:
                try:
                    observers_dict[model_name].stop()
                    observers_dict[model_name].join()
                    del observers_dict[model_name]
                except Exception as e:
                    logging.error(f"Error stopping watcher for {model_name}: {e}")
            
            # Remove from configuration
            del UNIFIED_CONFIGS[model_name]
            save_config(UNIFIED_CONFIGS)
            
            # Remove from stats
            if model_name in global_stats._stats:
                del global_stats._stats[model_name]
            
            logging.info(f"✅ Deleted model: {model_name}")
            
            return jsonify({"success": True, "message": f"Model {model_name} deleted successfully"})
            
    except Exception as e:
        logging.error(f"Error deleting model: {e}")
        return jsonify({"success": False, "error": str(e)})
    
@app.route('/api/update_sequences_bulk', methods=['POST'])
@admin_required
def update_sequences_bulk():
    """Bulk update sequence numbers for multiple models"""
    try:
        data = request.json
        updates = data.get('updates', [])
        
        if not updates:
            return jsonify({"success": False, "error": "No updates provided"})
        
        # ========== VALIDATION: Check for duplicate sequences ==========
        sequence_check = {}
        for update in updates:
            seq = update.get('sequence_number')
            model = update.get('model_name')
            
            if seq == 0:  # 0 is allowed for multiple models (disabled)
                continue
            
            if seq in sequence_check:
                return jsonify({
                    "success": False, 
                    "error": f"❌ Duplicate sequence number {seq} found!\n\n" +
                           f"Models: '{sequence_check[seq]}' and '{model}'\n\n" +
                           f"Each active model must have a unique sequence number.\n" +
                           f"(Only sequence 0 can be used multiple times to disable models)"
                })
            
            sequence_check[seq] = model
        
        # ========== END VALIDATION ==========
        
        updated_count = 0
        errors = []
        
        with config_lock:
            for update in updates:
                model_name = update.get('model_name')
                sequence_number = update.get('sequence_number')
                
                if not model_name:
                    errors.append(f"Missing model name in update")
                    continue
                
                try:
                    sequence_number = int(sequence_number)
                except (ValueError, TypeError):
                    errors.append(f"{model_name}: Invalid sequence number")
                    continue
                
                if model_name not in UNIFIED_CONFIGS:
                    errors.append(f"{model_name}: Model not found")
                    continue
                
                # Update sequence number
                UNIFIED_CONFIGS[model_name]['sequence_number'] = sequence_number
                
                # Update in stats
                if model_name in global_stats._stats:
                    global_stats._stats[model_name]["sequence_number"] = sequence_number
                
                updated_count += 1
                logging.info(f"✅ Updated sequence for {model_name}: {sequence_number}")
            
            # Save configuration
            if updated_count > 0:
                save_config(UNIFIED_CONFIGS)
                
                # Update max sequence in sequential processor
                sequential_processor.max_sequence = max(
                    [config.get('sequence_number', 999) for config in UNIFIED_CONFIGS.values()]
                )
        
        if errors:
            error_msg = "; ".join(errors)
            return jsonify({
                "success": False, 
                "error": f"Updated {updated_count} models, but encountered errors: {error_msg}"
            })
        
        return jsonify({
            "success": True, 
            "message": f"✅ Successfully updated {updated_count} model(s)"
        })
        
    except Exception as e:
        logging.error(f"Error in bulk sequence update: {e}")
        return jsonify({"success": False, "error": str(e)})

def run_flask():
    app.run(host=WEB_HOST, port=WEB_PORT, debug=False, use_reloader=False)


# ========================= MAIN =========================
def main():
    """Sequential processing main function - IMPROVED VERSION"""
    logging.info("=" * 80)
    logging.info("🔄 SEQUENTIAL CSV PROCESSOR - IMPROVED SIGNAL HANDLING")
    logging.info("=" * 80)
    
    # ===== ADDED: Log Python version and platform =====
    logging.info(f"🐍 Python: {sys.version}")
    logging.info(f"💻 Platform: {sys.platform}")
    
    # Start Flask
    flask_thread = Thread(target=run_flask, daemon=True, name="FlaskThread")
    flask_thread.start()
    logging.info(f"🌐 Dashboard: http://localhost:{WEB_PORT}")
    
    time.sleep(2)
    
    # Setup database connections
    for model_name, config in UNIFIED_CONFIGS.items():
        # ===== ADDED: Shutdown check =====
        if killer.kill_now or shutdown_event.is_set():
            break
            
        try:
            conn_str = build_connection_string(
                config["db_driver"], config["db_server"], 
                config["db_name"], config["db_username"], config["db_password"]
            )
            
            success, message = test_database_connection(
                config["db_driver"], config["db_server"], 
                config["db_name"], config["db_username"], config["db_password"]
            )
            
            if success:
                logging.info(f"✅ [{model_name}] DB connected")
            else:
                logging.error(f"❌ [{model_name}] DB failed: {message}")
        except Exception as e:
            logging.error(f"❌ [{model_name}] Setup error: {e}")
    
    # Start file watchers (for monitoring only - processing is sequential)
    for model_name, config in UNIFIED_CONFIGS.items():
        # ===== ADDED: Shutdown check =====
        if killer.kill_now or shutdown_event.is_set():
            break
            
        observer = start_watcher_for_model(model_name, config)
        if observer:
            logging.info(f"✅ [{model_name}] Monitor started")
    
    # Start sequential processor
    logging.info("🚀 Starting sequential processor...")
    
    # ===== ADDED: Try-except for better error handling =====
    try:
        sequential_processor.process_sequential_loop()
    except KeyboardInterrupt:
        logging.info("⏹️ KeyboardInterrupt received in main()")
    except Exception as e:
        logging.error(f"❌ Fatal error in main: {e}", exc_info=True)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("⏹️ Shutdown requested...")
    except Exception as e:
        logging.error(f"❌ Fatal error: {e}", exc_info=True)
    finally:
        # ===== ADDED: Better shutdown handling =====
        logging.info("🧹 Starting cleanup...")
        
        shutdown_event.set()
        sequential_processor.stop()
        
        # Save stats before shutdown
        logging.info("💾 Saving stats history...")
        try:
            global_stats.force_save()
            logging.info("✅ Stats saved successfully")
        except Exception as e:
            logging.error(f"⚠️ Error saving stats: {e}")
        
        # Stop observers with timeout
        logging.info("🛑 Stopping file watchers...")
        for model_name, observer in list(observers_dict.items()):
            try:
                observer.stop()
                observer.join(timeout=3)  # ===== CHANGED: Don't wait forever =====
                logging.info(f"✅ Stopped watcher for {model_name}")
            except Exception as e:
                logging.warning(f"⚠️ Error stopping watcher {model_name}: {e}")
        
        logging.info("✅ Shutdown complete")
        sys.exit(0)  # ===== ADDED: Explicit exit =====