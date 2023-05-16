install:
	# install dependencies
	pip install --upgrade pip &&\
		pip --default-timeout=1000 install -r requirements.txt 
format:
	# format python code with black
	black *.py
lint:
	# check code syntaxes
	pylint --disable=R,C *.py

environment:
	# create environment variables	
	chmod +x environment.sh &&\
		./environment.sh