install:
	python3 -m pip install -r requirements.txt
init:
	python3 init_database.py
update:
	python3 update.py
test:
	python3 backtest.py
