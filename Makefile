# Formatting
format:
	poetry run black datavault/
	poetry run isort datavault/
	poetry run flake8 datavault/