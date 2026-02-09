#!/usr/bin/env python3
"""
Video Converter App - Desktop GUI for converting MKV to MP4 and uploading to Google Drive.
"""

import os
import sys
import subprocess
import threading
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import queue
import signal

# Set the app name in macOS Dock
if sys.platform == 'darwin':
    try:
        from Foundation import NSBundle
        bundle = NSBundle.mainBundle()
        info = bundle.localizedInfoDictionary() or bundle.infoDictionary()
        if info:
            info['CFBundleName'] = 'Video Converter'
    except ImportError:
        pass

    # Also try using ctypes to set process name
    try:
        import ctypes
        libc = ctypes.CDLL('libc.dylib')
        # Set process name
        libc.setprogname(b'Video Converter')
    except:
        pass

class VideoConverterApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Video Converter")
        self.root.geometry("500x400")
        self.root.resizable(True, True)

        # State
        self.conversion_process = None
        self.is_running = False
        self.output_queue = queue.Queue()

        # Default paths
        self.rclone_remote = "gdrive:"
        self.default_folder = "1 - Skydiving Competitions/2018WPC - Organized"

        self.setup_ui()
        self.check_output_queue()

    def setup_ui(self):
        # Main container
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)

        # Folder selection
        folder_frame = ttk.LabelFrame(main_frame, text="Google Drive Folder", padding="5")
        folder_frame.pack(fill=tk.X, pady=(0, 10))

        self.folder_var = tk.StringVar(value=self.default_folder)
        self.folder_entry = ttk.Entry(folder_frame, textvariable=self.folder_var, width=50)
        self.folder_entry.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=(0, 5))

        browse_btn = ttk.Button(folder_frame, text="Browse", command=self.browse_folder)
        browse_btn.pack(side=tk.RIGHT)

        # Quick folder buttons
        quick_frame = ttk.Frame(main_frame)
        quick_frame.pack(fill=tk.X, pady=(0, 10))

        ttk.Button(quick_frame, text="2018 WPC",
                   command=lambda: self.set_folder("1 - Skydiving Competitions/2018WPC - Organized")).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, text="2016 Mondial",
                   command=lambda: self.set_folder("1 - Skydiving Competitions/2016 Mondial - Organized")).pack(side=tk.LEFT, padx=2)
        ttk.Button(quick_frame, text="2018 USPA Nats",
                   command=lambda: self.set_folder("1 - Skydiving Competitions/2018 USPA Nationals - Organized")).pack(side=tk.LEFT, padx=2)

        # Control buttons
        btn_frame = ttk.Frame(main_frame)
        btn_frame.pack(fill=tk.X, pady=(0, 10))

        self.start_btn = ttk.Button(btn_frame, text="Start Conversion", command=self.start_conversion)
        self.start_btn.pack(side=tk.LEFT, padx=5)

        self.stop_btn = ttk.Button(btn_frame, text="Stop", command=self.stop_conversion, state=tk.DISABLED)
        self.stop_btn.pack(side=tk.LEFT, padx=5)

        self.status_btn = ttk.Button(btn_frame, text="Check Status", command=self.check_status)
        self.status_btn.pack(side=tk.LEFT, padx=5)

        self.cleanup_btn = ttk.Button(btn_frame, text="Cleanup Empty Folders", command=self.cleanup_folders)
        self.cleanup_btn.pack(side=tk.LEFT, padx=5)

        # Status display
        status_frame = ttk.LabelFrame(main_frame, text="Status", padding="5")
        status_frame.pack(fill=tk.X, pady=(0, 10))

        self.status_label = ttk.Label(status_frame, text="Ready", font=('Helvetica', 12))
        self.status_label.pack(anchor=tk.W)

        self.progress_label = ttk.Label(status_frame, text="MKV: -- | MP4: --")
        self.progress_label.pack(anchor=tk.W)

        # Log output
        log_frame = ttk.LabelFrame(main_frame, text="Log", padding="5")
        log_frame.pack(fill=tk.BOTH, expand=True)

        self.log_text = tk.Text(log_frame, height=10, width=60, font=('Monaco', 10))
        self.log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)

        scrollbar = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self.log_text.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.log_text.config(yscrollcommand=scrollbar.set)

    def set_folder(self, folder):
        self.folder_var.set(folder)

    def browse_folder(self):
        # List folders from Google Drive
        self.log("Fetching folders from Google Drive...")
        try:
            result = subprocess.run(
                f'rclone lsf "{self.rclone_remote}1 - Skydiving Competitions" --dirs-only 2>/dev/null',
                shell=True, capture_output=True, text=True, timeout=30
            )
            folders = [f.strip('/') for f in result.stdout.strip().split('\n') if f.strip()]

            if folders:
                # Show selection dialog
                dialog = tk.Toplevel(self.root)
                dialog.title("Select Folder")
                dialog.geometry("400x300")

                listbox = tk.Listbox(dialog, font=('Helvetica', 11))
                listbox.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

                for folder in folders:
                    listbox.insert(tk.END, folder)

                def on_select():
                    selection = listbox.curselection()
                    if selection:
                        selected = listbox.get(selection[0])
                        self.folder_var.set(f"1 - Skydiving Competitions/{selected}")
                    dialog.destroy()

                ttk.Button(dialog, text="Select", command=on_select).pack(pady=5)
            else:
                self.log("No folders found")
        except Exception as e:
            self.log(f"Error: {e}")

    def log(self, message):
        self.log_text.insert(tk.END, f"{message}\n")
        self.log_text.see(tk.END)

    def start_conversion(self):
        if self.is_running:
            messagebox.showwarning("Warning", "Conversion is already running")
            return

        folder = self.folder_var.get()
        if not folder:
            messagebox.showerror("Error", "Please select a folder")
            return

        self.is_running = True
        self.start_btn.config(state=tk.DISABLED)
        self.stop_btn.config(state=tk.NORMAL)
        self.status_label.config(text="Converting...")
        self.log(f"Starting conversion for: {folder}")

        # Start conversion in background thread
        thread = threading.Thread(target=self.run_conversion, args=(folder,), daemon=True)
        thread.start()

    def run_conversion(self, folder):
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            script_path = os.path.join(script_dir, "convert_and_upload.py")

            self.conversion_process = subprocess.Popen(
                [sys.executable, "-u", script_path, folder],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )

            for line in iter(self.conversion_process.stdout.readline, ''):
                if line:
                    self.output_queue.put(line.strip())
                if not self.is_running:
                    break

            self.conversion_process.wait()
            self.output_queue.put("__DONE__")

        except Exception as e:
            self.output_queue.put(f"Error: {e}")
            self.output_queue.put("__DONE__")

    def check_output_queue(self):
        try:
            while True:
                message = self.output_queue.get_nowait()
                if message == "__DONE__":
                    self.is_running = False
                    self.start_btn.config(state=tk.NORMAL)
                    self.stop_btn.config(state=tk.DISABLED)
                    self.status_label.config(text="Completed")
                else:
                    self.log(message)
        except queue.Empty:
            pass

        self.root.after(100, self.check_output_queue)

    def stop_conversion(self):
        if self.conversion_process:
            self.log("Stopping conversion...")
            self.conversion_process.terminate()
            self.is_running = False
            self.start_btn.config(state=tk.NORMAL)
            self.stop_btn.config(state=tk.DISABLED)
            self.status_label.config(text="Stopped")

    def check_status(self):
        self.log("Checking status...")
        folder = self.folder_var.get()

        def get_status():
            try:
                # Get MKV count
                mkv_result = subprocess.run(
                    f'rclone lsf "{self.rclone_remote}{folder}" -R 2>/dev/null | grep -ic "\\.mkv$"',
                    shell=True, capture_output=True, text=True, timeout=60
                )
                mkv_count = mkv_result.stdout.strip() or "0"

                # Get MP4 count
                mp4_result = subprocess.run(
                    f'rclone lsf "{self.rclone_remote}{folder}" -R 2>/dev/null | grep -ic "\\.mp4$"',
                    shell=True, capture_output=True, text=True, timeout=60
                )
                mp4_count = mp4_result.stdout.strip() or "0"

                # Check running processes
                ps_result = subprocess.run(
                    'ps aux | grep convert_and_upload | grep -v grep | wc -l',
                    shell=True, capture_output=True, text=True
                )
                running = int(ps_result.stdout.strip() or "0")

                self.root.after(0, lambda: self.update_status(mkv_count, mp4_count, running))

            except Exception as e:
                self.root.after(0, lambda: self.log(f"Error checking status: {e}"))

        thread = threading.Thread(target=get_status, daemon=True)
        thread.start()

    def update_status(self, mkv_count, mp4_count, running):
        self.progress_label.config(text=f"MKV remaining: {mkv_count} | MP4 done: {mp4_count}")
        status = f"Running ({running} workers)" if running > 0 else "Idle"
        self.status_label.config(text=status)
        self.log(f"Status: MKV={mkv_count}, MP4={mp4_count}, Workers={running}")

    def cleanup_folders(self):
        self.log("Cleaning up empty folders...")
        folder = self.folder_var.get()

        def do_cleanup():
            try:
                # Use rclone rmdirs to remove empty directories
                result = subprocess.run(
                    f'rclone rmdirs "{self.rclone_remote}{folder}" --leave-root 2>&1',
                    shell=True, capture_output=True, text=True, timeout=120
                )

                if result.returncode == 0:
                    self.root.after(0, lambda: self.log("Empty folders cleaned up"))
                else:
                    self.root.after(0, lambda: self.log(f"Cleanup result: {result.stderr or 'Done'}"))

            except Exception as e:
                self.root.after(0, lambda: self.log(f"Error: {e}"))

        thread = threading.Thread(target=do_cleanup, daemon=True)
        thread.start()

def main():
    root = tk.Tk()

    # Set macOS-specific options
    if sys.platform == 'darwin':
        root.createcommand('tk::mac::Quit', root.destroy)

    app = VideoConverterApp(root)
    root.mainloop()

if __name__ == "__main__":
    main()
