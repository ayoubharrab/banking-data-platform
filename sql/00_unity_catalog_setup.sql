CREATE CATALOG IF NOT EXISTS banking
  MANAGED LOCATION 'abfss://gold@stbankingayoub.dfs.core.windows.net/_catalog'
  COMMENT 'Banking Data Platform - Berka dataset';

CREATE SCHEMA IF NOT EXISTS banking.bronze
  MANAGED LOCATION 'abfss://bronze@stbankingayoub.dfs.core.windows.net/tables'
  COMMENT 'Raw data in Delta format';

CREATE SCHEMA IF NOT EXISTS banking.silver
  MANAGED LOCATION 'abfss://silver@stbankingayoub.dfs.core.windows.net/tables'
  COMMENT 'Cleaned and conformed data';

CREATE SCHEMA IF NOT EXISTS banking.gold
  MANAGED LOCATION 'abfss://gold@stbankingayoub.dfs.core.windows.net/tables'
  COMMENT 'Star schema for Power BI';