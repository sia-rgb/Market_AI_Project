@echo off
cd /d "%~dp0"
call .venv\Scripts\activate
python data_interpreter.py
python insight_generator.py
for %%f in (Market_Insight_Report_*.pdf) do copy "%%f" "%USERPROFILE%\Desktop\" /Y 2>nul
for %%f in (Market_Insight_Report_*.md) do copy "%%f" "%USERPROFILE%\Desktop\" /Y 2>nul
