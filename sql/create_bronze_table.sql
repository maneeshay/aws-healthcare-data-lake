CREATE EXTERNAL TABLE healthcare_bronze_db.cms_providers_bronze (
    Rndrng_NPI                     string,
    Rndrng_Prvdr_Last_Org_Name     string,
    Rndrng_Prvdr_First_Name        string,
    Rndrng_Prvdr_MI                string,
    Rndrng_Prvdr_Crdntls           string,
    Rndrng_Prvdr_Gndr              string,
    Rndrng_Prvdr_Ent_Cd            string,
    Rndrng_Prvdr_St1               string,
    Rndrng_Prvdr_St2               string,
    Rndrng_Prvdr_City              string,
    Rndrng_Prvdr_State_Abrvtn      string,
    Rndrng_Prvdr_State_FIPS        string,
    Rndrng_Prvdr_Zip5              string,
    Rndrng_Prvdr_RUCA              string,
    Rndrng_Prvdr_RUCA_Desc         string,
    Rndrng_Prvdr_Cntry             string,
    Rndrng_Prvdr_Type              string,
    Rndrng_Prvdr_Mdcr_Prtcptg_Ind string,
    Tot_HCPCS_Cds                  string,
    Tot_Benes                      string,
    Tot_Srvcs                      string,
    Tot_Sbmtd_Chrg                 string,
    Tot_Mdcr_Alowd_Amt             string,
    Tot_Mdcr_Pymt_Amt              string,
    Tot_Mdcr_Stdzd_Amt             string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION 's3://aws-healthcare-data-lake/bronze/cms_providers/year=2024/'
TBLPROPERTIES ('skip.header.line.count'='1');