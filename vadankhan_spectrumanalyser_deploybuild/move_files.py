import os
import shutil
from pathlib import Path

ROOT_DIR = Path(os.getcwd())
EXPORTS_FILE_PATH = ROOT_DIR / "PROCESSED_SPECTRA"

source_folder = EXPORTS_FILE_PATH
destination_folder = r"K:\File_Transfer\LIV\Processed Spectra"


for filename in os.listdir(source_folder):
    if not os.access(destination_folder, os.W_OK):
        print("Destination not writable!")
        break

    if filename.lower().endswith((".txt", ".log")):
        continue  # Skip .txt and .log files

    src_file = os.path.join(source_folder, filename)
    dst_file = os.path.join(destination_folder, filename)

    try:
        # Optional: skip if destination file exists
        if os.path.exists(dst_file):
            print(f"Skipping (already exists): {filename}")
            continue

        shutil.copy2(src_file, dst_file)  # copy with metadata
        os.remove(src_file)  # delete original
        print(f"Moved: {filename}")
    except PermissionError as e:
        print(f"Permission denied: {dst_file} — {e}")
    except Exception as e:
        print(f"Error moving {filename}: {e}")
