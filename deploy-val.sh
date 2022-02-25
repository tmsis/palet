#!/bin/zsh
# Usage deploy-val.bat <version>
# e.g. deploy-val.bat 1.2.1

ver=$1
if [ -z "$ver" ]
then
echo "Usage: deploy-val.sh <version number>"
exit
else
echo "*********** cleaning up cache *************"
rm -rf ./build; rm -rf /*.egg-info
echo "*********** building new whl file *********"
python ./setup.py bdist_wheel
echo "*********** copying newly built wheel to databricks**********"
databricks --profile val fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-$1-py3-none-any.whl --overwrite
print  palet-$ver-py3-none-any.whl
fi
