init:
	pip install pipenv --upgrade
	pipenv install --dev --skip-lock

test:
	pipenv run detox

# E501 is "line longer than 80 chars" but the automated fix is ugly.
flake8:
	pipenv run flake8 --ignore=E501

test-readme:
	@pipenv run python setup.py check -r -s && ([ $$? -eq 0 ] && echo "README.rst ok") || echo "Invalid markup in README.rst!"

publish:
	pip install 'twine>=1.5.0'
	python setup.py sdist
	twine upload dist/*
	rm -rf build dist .egg kylinpy.egg-info
