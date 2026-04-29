@echo off
echo Starting Report Generator...
pip install -r requirements.txt --quiet
start http://localhost:5000
python app.py
