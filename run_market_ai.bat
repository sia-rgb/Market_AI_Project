@echo off
REM 建议在任务计划程序中设置为每周五 18:00 执行
cd /d "%~dp0"
call .venv\Scripts\activate
python data_interpreter.py
python insight_generator.py
explorer "%~dp0每周市场速览"
