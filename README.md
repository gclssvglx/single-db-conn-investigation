# Python template

An opinionated Python template that comes pre-configured with [`coverage`](https://pypi.org/project/coverage/), [`pylint`](https://pypi.org/project/pylint/) and [`autopep8`](https://pypi.org/project/autopep8/). It also has a simple Python file and test to get you started.

## Requirements

You'll need to have [`pyenv`](https://github.com/pyenv/pyenv) installed and a version of Python within `pyenv`.

Once this is done, you should have the Python Standard Library available and be able to run the following commands...

```shell
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
python -m unittest discover
coverage run -m unittest discover
coverage report -m
autopep8 --in-place --aggressive --aggressive src/*.py test/*.py
pylint --recursive=y ./src ./test
```

## Aliases

These commands are quite long and tricky to type. You might consider using the following aliases...

```shell
alias py-up='source venv/bin/activate'
alias py-down='deactivate'
alias py-bundle='pip install -r requirements.txt'
alias py-test='python -m unittest discover'
alias py-cov='coverage run -m unittest discover && coverage report -m'
alias py-cop='pylint --recursive=y ./src ./test'
```