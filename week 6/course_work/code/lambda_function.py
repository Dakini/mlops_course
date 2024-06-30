import os

import model

PREDICTIONS_STREAM_NAME = os.getenv("PREDICTIONS_STREAM_NAME", "ride_predictions")
RUN_ID = os.getenv("RUN_ID")
TEST_RUN = os.getenv("TEST_RUN", "False") == "True"


model_service = model.init(PREDICTIONS_STREAM_NAME, RUN_ID, TEST_RUN)


def lambda_handler(event, context):
    # pylint: disable=unused-argument

    # print(json.dumps(event))
    return model_service.lambda_handler(event)
