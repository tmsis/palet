echo off
setlocal
@REM Usage deploy-palet.bat <version>
@REM e.g. deploy-palet .bat 1.5.20220311

set ver=%1
set svr=%2
if not defined ver goto :usage
if defined ver goto :proceed
:usage
echo off
echo "Usage: deploy-val.bat <version number>"
exit /B 1
:proceed
echo on
del /s /f /Q .\build; del /s /f /Q .\*.egg-info
python setup.py bdist_wheel
echo off
if defined svr goto :publish
:publish
echo on
databricks --profile val fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-%1-py3-none-any.whl --overwrite
@REM databricks --profile prod fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-%1-py3-none-any.whl --overwrite
