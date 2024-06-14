FROM python:3.12-slim

RUN pip install -U pip
RUN pip install pipenv

WORKDIR /app
COPY ["Pipfile", "Pipfile.lock", "./"]
COPY ["lin_reg.bin", "predict.py", "./"]
# install to the main python system in docker as this is self contained 
RUN pipenv install --system --deploy
#expose the port for local host
EXPOSE 9696
ENTRYPOINT [ "gunicorn" ,"--bind=0.0.0.0:9696", "predict:app" ]
