databricks --profile val fs cp ./palet/Enrollment.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/PaletMetadata.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/EligibilityType.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Paletable.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite  
databricks --profile val fs cp ./palet/Palet.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Diagnoses.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/CoverageType.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/EnrollmentType.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/ServiceCategory.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/DateDimension.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/Readmits.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite
databricks --profile val fs cp ./palet/ClaimsAnalysis.py dbfs:/FileStore/shared_uploads/akira/lib/palet/ --overwrite



Write-Output "last updated on: " 
Get-Date -Format "dddd MM/dd/yyyy HH:mm K"