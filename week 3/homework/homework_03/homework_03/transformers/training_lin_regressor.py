from sklearn.feature_extraction import DictVectorizer
from sklearn.linear_model import LinearRegression

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def train_model(data, *args, **kwargs):
    categorical = ['PULocationID', 'DOLocationID']
    #create data dict
    data_dict = data[categorical].to_dict(orient='records')

    #create the dv 
    dv = DictVectorizer()

    X_train  = dv.fit_transform(data_dict)
    y_train = data['duration'].values

    lr = LinearRegression()
    lr.fit(X_train, y_train)
    print(lr.intercept_)

    return lr, dv


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
