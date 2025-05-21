import os
import sys
from pathlib import Path
import time
import csv
from datetime import datetime
import threading


# import threading
# import multiprocessing
# import concurrent.futures

import numpy as np
import pandas as pd

# import matplotlib.pyplot as plt
# import dask.dataframe as dd
# import polars as pl
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# import pyarrow as pa
# import pyarrow.csv as csv
from scipy.signal import find_peaks

# import requests
# from bs4 import BeautifulSoup

CURRENT_DIR = Path(os.getcwd())
# Move to the root directory
ROOT_DIR = CURRENT_DIR

# RAW_FILE_PATH = ROOT_DIR / "wavelength_spectra_files"
# RAW_FILE_PATH.mkdir(parents=True, exist_ok=True)
EXPORTS_FILE_PATH = ROOT_DIR / "PROCESSED_SPECTRA"
EXPORTS_FILE_PATH.mkdir(parents=True, exist_ok=True)

# Add the root directory to the system path

sys.path.append(str(ROOT_DIR))


ANALYSIS_RUN_NAME = "PROCESSED_SPECTRA"

SUBARU_DECODER = "QC WAFER_LAYOUT 24Dec.csv"
HALO_DECODER = "HALO_DECODER_NE-rev1_1 logic_coords_annotated.csv"


# ------------------------------------ Spectrum Analysis Code ------------------------------------ #
def load_decoder(decoder_file_path):
    print(f"Loading decoder from: {decoder_file_path}")
    start_time = time.time()

    if not decoder_file_path.exists():
        print(f"Decoder file not found at {decoder_file_path}")
        raise ValueError("Decoder is empty")

    df_decoder = pd.read_csv(decoder_file_path, usecols=["Logic_X", "Logic_Y", "TE_LABEL", "TYPE"])
    df_decoder = df_decoder.set_index(["Logic_X", "Logic_Y"])

    end_time = time.time()
    print(f"Loaded in {end_time - start_time:.2f} seconds.\n")
    return df_decoder


def transform_raw_file(
    filepath,
    wafer_id,
    decoder_df,
    wavelength_lb=824,
    wavelength_ub=832,
    chunksize=1000,
    max_chunks=400,
):
    print(f"Starting file transformation for {wafer_id}...")
    total_t0 = time.time()

    t1 = time.time()
    col_names = pd.read_csv(filepath, nrows=1).columns
    intensity_cols = [col for col in col_names if col.startswith("Intensity_")]
    wavelengths = {col: float(col.split("_")[1]) for col in intensity_cols}
    selected_intensity_cols = [col for col, wl in wavelengths.items() if wavelength_lb <= wl <= wavelength_ub]
    usecols = ["X", "Y"] + selected_intensity_cols
    data_points_threshold = len(selected_intensity_cols)
    print(f"Header parsing and column filtering took {time.time() - t1:.2f} s")

    with pd.read_csv(filepath, chunksize=chunksize, usecols=usecols) as reader:
        for i, chunk in enumerate(reader):
            if i >= max_chunks:
                break

            # Base transformation
            t_base = time.time()
            long_df = chunk.melt(
                id_vars=["X", "Y"],
                value_vars=selected_intensity_cols,
                var_name="Wavelength",
                value_name="Intensity",
            )
            long_df["Wavelength"] = long_df["Wavelength"].map(wavelengths)
            long_df = long_df.merge(decoder_df, left_on=["X", "Y"], right_index=True, how="left")
            long_df = long_df.drop(columns=["X", "Y"])
            long_df = long_df[["TYPE", "TE_LABEL", "Wavelength", "Intensity"]]
            t_base_elapsed = time.time() - t_base

            yield long_df, data_points_threshold, t_base_elapsed

            # print(f"Chunk {i+1} | Base transform: {t_base_elapsed:.2f}s | Total time: {time.time() - chunk_start:.2f}s")

    print(f"File transformation for {wafer_id} completed in {time.time() - total_t0:.2f} seconds.")


def extract_top_two_peaks(df_group):
    """
    Detects the top two peaks in a spectrum using linear intensity values.
    Returns:
        peak_series (pd.Series): Summary of top peaks and SMSR.
        timing_info (dict): Time taken for each step.
    """
    t_start = time.time()
    timing = {}

    # Sort the dataframe by wavelength
    t0 = time.time()
    df_sorted = df_group.sort_values("Wavelength")
    timing["sort"] = time.time() - t0

    # Extract arrays and find peaks
    t0 = time.time()
    intensities = df_sorted["Intensity"].values
    wavelengths = df_sorted["Wavelength"].values
    peak_indices, _ = find_peaks(intensities)
    timing["find_peaks"] = time.time() - t0

    # Handle no peaks case
    t0 = time.time()
    if len(peak_indices) == 0:
        peak_series = pd.Series(
            {
                "highest_peak_wavelength": np.nan,
                "highest_peak_intensity_linear": np.nan,
                "second_peak_wavelength": np.nan,
                "second_peak_intensity_linear": np.nan,
                "SMSR_dB": np.nan,
                "SMSR_linear": np.nan,
            }
        )
        timing["ordering"] = 0.0
        timing["extraction"] = 0.0
        return peak_series, timing

    # Sort peak indices by descending intensity
    sorted_order = np.argsort(intensities[peak_indices])[::-1]
    timing["ordering"] = time.time() - t0

    # Extract peaks and compute SMSR
    t0 = time.time()
    highest_idx = peak_indices[sorted_order[0]]
    highest_peak_wavelength = wavelengths[highest_idx]
    highest_peak_intensity_linear = intensities[highest_idx]

    if len(sorted_order) > 1:
        second_idx = peak_indices[sorted_order[1]]
        second_peak_wavelength = wavelengths[second_idx]
        second_peak_intensity_linear = intensities[second_idx]

        # dB value of second peak relative to highest
        second_peak_dB = 10 * np.log10(second_peak_intensity_linear / highest_peak_intensity_linear)

        SMSR_dB = -second_peak_dB
        SMSR_linear = highest_peak_intensity_linear / second_peak_intensity_linear
    else:
        second_peak_wavelength = np.nan
        second_peak_intensity_linear = np.nan
        SMSR_dB = np.nan
        SMSR_linear = np.nan

    peak_series = pd.Series(
        {
            "highest_peak_wavelength": highest_peak_wavelength,
            "highest_peak_intensity_linear": highest_peak_intensity_linear,
            "second_peak_wavelength": second_peak_wavelength,
            "second_peak_intensity_linear": second_peak_intensity_linear,
            "SMSR_dB": SMSR_dB,
            "SMSR_linear": SMSR_linear,
        }
    )
    timing["extraction"] = time.time() - t0

    return peak_series, timing


def process_export_and_peaks(filepath, wafer_code, tool_name, decoder_df):
    print(f"\n=== Starting processing for {wafer_code} ===")
    total_t0 = time.time()

    peak_output_path = EXPORTS_FILE_PATH / f"{ANALYSIS_RUN_NAME}_{tool_name}_{wafer_code}_headlevel_SMSR.csv"

    accumulator = {}
    data_point_count = {}
    chunk_counter = 0

    peak_columns = [
        "LOT",
        "TE_LABEL",
        "Highest Peak (Wavelength)",
        "Highest Peak (Linear Intensity)",
        "Second Peak (Wavelength)",
        "Second Peak (Linear Intensity)",
        "SMSR_dB",
        "SMSR_linear",
    ]

    with open(peak_output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(peak_columns)

    completed_labels = 0
    peak_buffer = []

    for chunk, data_points_threshold, base_time in transform_raw_file(filepath, wafer_code, decoder_df):
        chunk_counter += 1
        chunk_start = time.time()

        t_peaks_breakdown = {}
        t_peak_total = 0
        t_actual_write_total = 0
        t_gather_total = 0

        for te_label, group in chunk.groupby("TE_LABEL"):
            if te_label not in accumulator:
                accumulator[te_label] = [group]
                data_point_count[te_label] = len(group)
            else:
                accumulator[te_label].append(group)
                data_point_count[te_label] += len(group)

            if data_point_count[te_label] >= data_points_threshold:
                t_gather_start = time.time()
                full_data = pd.concat(accumulator[te_label], ignore_index=True)
                t_gather_end = time.time()
                t_gather_total += t_gather_end - t_gather_start

                # Peak extraction
                t_peak_start = time.time()
                peak_series, smsrs = extract_top_two_peaks(full_data)
                t_peak_end = time.time()
                t_peak_total += t_peak_end - t_peak_start

                for k, v in smsrs.items():
                    t_peaks_breakdown[k] = t_peaks_breakdown.get(k, 0.0) + v

                # Assign TE_LABEL and LOT
                peak_series["TE_LABEL"] = te_label
                peak_series["LOT"] = wafer_code

                # Reorder the Series
                peak_series = peak_series[
                    ["LOT", "TE_LABEL"] + [col for col in peak_series.index if col not in {"LOT", "TE_LABEL"}]
                ]
                peak_buffer.append(peak_series)

                completed_labels += 1
                del accumulator[te_label]
                del data_point_count[te_label]

                if completed_labels >= 1000:
                    t_actual_write_start = time.time()
                    pd.DataFrame(peak_buffer).to_csv(peak_output_path, mode="a", header=False, index=False)
                    t_actual_write_end = time.time()
                    t_actual_write_total += t_actual_write_end - t_actual_write_start

                    peak_buffer.clear()
                    completed_labels = 0

        chunk_total = time.time() - chunk_start
        print(f"{wafer_code}: Chunk {chunk_counter} Summary:")
        print(f"  Base transform: {base_time:.2f}s")
        print(f"  Gather Laser Data Total: {t_gather_total:.2f}s")
        print(f"  Peak Calculation Total: {t_peak_total:.2f}s")
        print(f"  Peak detection breakdown:")
        for step, t in t_peaks_breakdown.items():
            print(f"    {step:>10}: {t:.2f}s")
        print(f"  Actual writing time: {t_actual_write_total:.2f}s")
        print(f"  Chunk total:    {chunk_total:.2f}s\n")

    # Final flush
    if peak_buffer:
        pd.DataFrame(peak_buffer).to_csv(peak_output_path, mode="a", header=False, index=False)

    print(f"=== Completed processing {wafer_code} in {time.time() - total_t0:.2f} seconds ===")


# ------------------------------------- Watchdog Calling Code ------------------------------------ #
monitored_folder = ROOT_DIR.parent / "GTX_Archived"
log_path = EXPORTS_FILE_PATH / "spectrum_analyser_log.txt"


def print_watcher_banner():
    print(
        f"\n\n# --------------------------- LIV Automatic Spectra Analyser (Vadan Khan) v2.4 -------------------------- #"
    )
    print("(Do not close this command window)")
    print(f"Watching folder: {monitored_folder}")


print_watcher_banner()


# Updated wafer code extractor from LIV CSV filename
def extract_testinfo_from_liv(folder_path):
    for file in Path(folder_path).iterdir():
        if file.name.startswith("LIV_") and file.suffix == ".csv":
            parts = file.name.split("_")
            if len(parts) >= 3:
                tool_name = parts[0] + "_" + parts[1]  # e.g., LIV_53
                wafer_code = parts[2]  # e.g., QCI44

                # Search for COD variants in the entire filename
                filename_upper = file.name.upper()
                if "COD250" in filename_upper:
                    test_type = "COD250"
                elif "COD70" in filename_upper:
                    test_type = "COD70"
                elif "COD" in filename_upper:
                    test_type = "COD"
                else:
                    test_type = "UNKNOWN"

                return tool_name, wafer_code, test_type
    return None, None, None


def wait_for_raw_csv_in_liv_folder(parent_folder, max_wait=300, delay=1):
    """
    Waits for any subfolder to appear inside parent_folder,
    and for 'Raw.csv' to become readable inside that subfolder.
    """
    wait_time = 0

    while wait_time < max_wait:
        subfolders = [sub for sub in parent_folder.iterdir() if sub.is_dir()]
        if subfolders:
            target_subfolder = subfolders[0]  # Assumes only one subfolder
            raw_csv_path = target_subfolder / "Raw.csv"

            if raw_csv_path.exists():
                try:
                    with open(raw_csv_path, "rb"):
                        return raw_csv_path  # File exists and is readable
                except (PermissionError, FileNotFoundError):
                    print(f"Waiting for Raw.csv to be readable... ({wait_time}s elapsed)")
            else:
                print(f"Waiting for Raw.csv to appear in {target_subfolder.name}... ({wait_time}s elapsed)")
        else:
            print(f"Waiting for subfolder to appear in {parent_folder.name}... ({wait_time}s elapsed)")

        time.sleep(delay)
        wait_time += delay

    return False  # Timeout


def initialise_spectra_processing(wafer_code, tool_name, detection_time, file_path):
    if wafer_code:
        product_code = wafer_code[:2]
        print(f"Extracted product code: {wafer_code}")

        # Select decoder file based on product code
        if product_code in ("QD", "NV"):
            decoder_path = ROOT_DIR / "decoders" / HALO_DECODER
        else:
            decoder_path = ROOT_DIR / "decoders" / SUBARU_DECODER

        decoder_df = load_decoder(decoder_path)

        process_export_and_peaks(file_path, wafer_code, tool_name, decoder_df)  # Main Spectra Processor

        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"[{detection_time}] ✅ Processed successfully: {file_path.name} (Wafer: {wafer_code})\n")
    else:
        message = f"No valid wafer code found in filename: {file_path.name}"
        print(message)
        with open(log_path, "a", encoding="utf-8") as log_file:
            log_file.write(f"[{detection_time}] ❌ {message}\n")


# Handler for new folders
class WaferFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            return

        def analysis_job():
            folder_path = Path(event.src_path)
            detection_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            with open(log_path, "a", encoding="utf-8") as log_file:
                log_file.write(f"[{detection_time}] Detected new folder: {folder_path.name}\n")

            print(f"\nDetected new wafer folder: {folder_path.name}")

            raw_csv_path = wait_for_raw_csv_in_liv_folder(folder_path)  # <-- store result here

            if not raw_csv_path:
                message = f"Raw.csv was never found or readable in: {folder_path.name}"
                print(message)
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"[{detection_time}] ❌ {message}\n")
                return

            tool_name, wafer_code, test_type = extract_testinfo_from_liv(folder_path)
            print(f"Detected Tool: {tool_name}, Wafer Code: {wafer_code}, Test Type: {test_type}")

            if test_type in ("COD70", "COD250"):
                message = f"Skipping analysis for test type {test_type} in: {folder_path.name}"
                print(message)
                with open(log_path, "a", encoding="utf-8") as log_file:
                    log_file.write(f"[{detection_time}] ⚠️ {message}\n")
                print_watcher_banner()
                return

            initialise_spectra_processing(wafer_code, tool_name, detection_time, raw_csv_path)
            print_watcher_banner()

        # Run the job in a background thread so Ctrl+C can still work
        threading.Thread(target=analysis_job, daemon=True).start()


# Watchdog setup
observer = Observer()
event_handler = WaferFileHandler()
observer.schedule(event_handler, str(monitored_folder), recursive=False)
observer.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    observer.stop()
observer.join()
