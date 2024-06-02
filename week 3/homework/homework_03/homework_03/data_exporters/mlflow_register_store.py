import mlflow
import pickle
mlflow.set_tracking_uri("http://mlflow:5001")
mlflow.set_experiment('Homework_03')


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    lr, dv = data
    print(lr, dv)
    with mlflow.start_run():
        model_info = mlflow.sklearn.log_model(
        sk_model=lr, artifact_path="model")
        with open("preprocessor.b", "wb") as f_out:
            pickle.dump(dv, f_out)
            mlflow.log_artifact("preprocessor.b", artifact_path="preprocessor")