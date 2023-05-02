use DATABASE silver;

create storage integration if not exists gcs_silver
  type = external_stage
  storage_provider = 'GCS'
  enabled = true
  storage_allowed_locations = ('gcs://pruebas-loliveros/silver');
  
DESC STORAGE INTEGRATION gcs_silver;