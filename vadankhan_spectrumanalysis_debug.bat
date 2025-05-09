@echo off
cd /d "C:\Users\762093\Documents\deployment-emulator-spectrumanalyser\Output\vadankhan-spectrumanalyser-deploybuild"
start /min cmd /c ".venv\Scripts\activate.bat && python vadankhan_spectrumanalyser_deploybuild\spectrum_files_transform_watchdog.py"
