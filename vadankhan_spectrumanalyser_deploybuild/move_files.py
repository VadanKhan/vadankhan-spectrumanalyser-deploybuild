import os
import shutil

source_folder = r"C:\Users\762093\Documents\deployment-emulator-spectrumanalyser\Output\vadankhan-spectrumanalyser-deploybuild\PROCESSED_SPECTRA"
destination_folder = r"J:\RND\Vadan Khan\PROCESSED_SPECTRA_SMSR"

for filename in os.listdir(source_folder):
    if filename == "spectrum_analyser_log.txt":
        continue  # Skip this specific file
    src_file = os.path.join(source_folder, filename)
    dst_file = os.path.join(destination_folder, filename)
    shutil.move(src_file, dst_file)
