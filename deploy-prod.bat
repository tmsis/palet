rm -r -fo .\build; rm -r -fo .\*.egg-info
python setup.py bdist_wheel
databricks --profile prod fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-1.2.20220130-py3-none-any.whl --overwrite
