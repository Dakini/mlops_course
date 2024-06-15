FROM agrigorev/zoomcamp-model:mlops-2024-3.10.13-slim
RUN pip install -U pip
RUN pip install pipenv

COPY ["starter.py","Pipfile", "Pipfile.lock", "./"]
RUN pipenv install --system --deploy

ENTRYPOINT [ "bash" ]