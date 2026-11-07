-- Table de traçabilité alimentée par scripts/run_elt.py (une ligne par table et par phase).
-- Lue en DirectQuery par le dashboard Power BI (dashbord/pipeline_dashboard.pbix).
IF OBJECT_ID('dbo.Extraction_Log', 'U') IS NULL
CREATE TABLE dbo.Extraction_Log (
    id               INT IDENTITY(1,1) PRIMARY KEY,
    execution_date   DATETIME2      NOT NULL,
    schema_name      NVARCHAR(128)  NULL,
    table_name       NVARCHAR(128)  NULL,
    status           NVARCHAR(20)   NULL,   -- success | error | skipped
    rows_extracted   INT            NULL,
    duration_seconds FLOAT          NULL,
    file_format      NVARCHAR(20)   NULL,
    file_path        NVARCHAR(500)  NULL,
    query_used       NVARCHAR(MAX)  NULL,
    executed_by      NVARCHAR(128)  NULL,
    message          NVARCHAR(MAX)  NULL,
    phase            NVARCHAR(20)   NULL,   -- extract | load | transform
    filename         NVARCHAR(260)  NULL,
    nombre_ligne     INT            NULL
);
