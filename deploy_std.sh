stty echo
databricks --profile val fs cp ./palet/Enrollment.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite 
databricks --profile val fs cp ./palet/PaletMetadata.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Eligibility.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Paletable.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite  
databricks --profile val fs cp ./palet/Coverage.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Paletable.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
