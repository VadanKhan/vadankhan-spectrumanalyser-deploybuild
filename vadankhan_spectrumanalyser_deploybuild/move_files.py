import os
import shutil

source_folder = r"C:\Path\To\Source"
destination_folder = r"C:\Path\To\Destination"

for filename in os.listdir(source_folder):
    src_file = os.path.join(source_folder, filename)
    dst_file = os.path.join(destination_folder, filename)
    shutil.move(src_file, dst_file)
