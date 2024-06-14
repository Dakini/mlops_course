\*\*Getting the model for deployment from MLflow

- Take the code from previous video
- Train another model and register with Mlflow
- Put the model into a scikit-leran pipeline
- Model Deployment with tracking server
- Model Deployment without the tracking server

starting the MlFlow server with S3:

```
mlflow server \
    --backend-store-uri=sqlite:///mlflow.db \
    --default-artifact-root=s3://mlflow--bucket/ --port 5001
```

Can also export the run ID and just import the env variable
