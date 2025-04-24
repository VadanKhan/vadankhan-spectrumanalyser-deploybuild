@echo off
cd /d "D:\Tester 58174\Output\PORTABLE-vadankhan-spectrumanalyser-deploybuild"
start /min cmd /c ".venv\Scripts\activate.bat && python vadankhan_spectrumanalyser_deploybuild\spectrum_files_transform_watchdog.py"
