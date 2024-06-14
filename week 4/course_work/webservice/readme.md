## deploying a model as a web-service

- Creating a virtual environment with Pipenv
- Creating a script for prediction
- Putting script into a flask app
- packaging the app to docker

```bash
docker build -t ride-duration-predict:v1 .
```

```bash
docker run -it --rm -p 9696:9696 ride-duration-predict:v1
```
