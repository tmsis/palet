%echo off
@REM Usage deploy-prod.bat <version>
@REM e.g. deploy-prod.bat 1.2.1

set ver=%1
if not defined ver goto :usage
if defined ver goto :proceed
:usage
%echo on
Usage: deploy-prod.bat <version number>
exit /B 1
:proceed
%echo on
del /s /f .\build; del /s /f .\*.egg-info
python setup.py bdist_wheel
databricks --profile prod fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-%1-py3-none-any.whl --overwrite