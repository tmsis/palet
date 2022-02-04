%echo off
@REM Usage deploy-val.bat <version>
@REM e.g. deploy-val.bat 1.2.1

set ver=%1
if not defined ver goto :usage
if defined ver goto :proceed
:usage
%echo on
Usage: deploy-val.bat <version number>
exit /B 1
:proceed
%echo on
<<<<<<< HEAD
del /s /f /o .\build; del /s /f /o .\*.egg-info
=======
del /s /f .\build; del /s /f .\*.egg-info
>>>>>>> 0b1f59e473df751c88622f707d64eb202ce303b9
python setup.py bdist_wheel
databricks --profile val fs cp ./dist/palet-1.0.1-py3-none-any.whl dbfs:/FileStore/shared_uploads/akira/lib/palet-%1-py3-none-any.whl --overwrite
