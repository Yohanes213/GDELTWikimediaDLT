-- Silver GDELT Secure View
CREATE OR REPLACE VIEW gdeltcatalog.gdeltdlt.silver_gdelt_secure_view
AS
SELECT
  Year,
  Action_Geo_FullName,
  event_timestamp,
  CASE
    WHEN is_member('compliance_auditors') THEN Source_URL
    ELSE 'Restricted'
  END AS SOURCEURL
FROM gdeltcatalog.gdeltdlt.gdelt_silver;

-- Grant permissions
GRANT SELECT ON gdeltcatalog.gdeltdlt.silver_gdelt_secure_view TO `compliance_auditors`;

