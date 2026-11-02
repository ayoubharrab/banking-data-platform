# Architecture

```mermaid
flowchart LR
    A[Berka CSV files] -->|Azure Data Factory| B[(ADLS Gen2 - raw)]
    B -->|Auto Loader| C[Bronze]
    C -->|PySpark cleaning| D[Silver]
    D -->|Star schema| E[Gold]
    E --> F[Power BI]
```