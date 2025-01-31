from kaggle.api.kaggle_api_extended import KaggleApi

# Authenticate with Kaggle using the Kaggle API.
# Docs here: https://github.com/Kaggle/kaggle-api/blob/main/docs/README.md#api-credentials

# Can edit this method later to be something more useable?
api = KaggleApi()
api.authenticate()

api.dataset_download_files("mlg-ulb/creditcardfraud", path="./data/", unzip=True)


