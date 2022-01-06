@REM databricks --profile dev fs cp .\palet\cfg\apdxc.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\countystate_lookup.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\fmg.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\missVar.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\prgncy.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\prvtxnmy.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\sauths.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\schip.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\splans.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\st_fips.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\st_name.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\st_usps.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\st2_name.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\stabr.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\stc_cd.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\zcc.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite
@REM databricks --profile dev fs cp .\palet\cfg\zipstate_lookup.pkl dbfs:/FileStore/shared_uploads/akira/lib/palet/cfg/ --overwrite


databricks --profile dev fs cp .\palet\Palet.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile dev fs cp .\palet\Article.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile dev fs cp .\palet\Enrollment.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
