graph TD
    A[QER Dataset] --> B[Get Latest Timestamp]
    A --> C[Get Latest Batch Records]
    B --> C
    C --> D[Rank Records by Account/Region/ASV]
    D --> E[Calculate Resource Counts]
    E --> F[Compute Evaluation Coverage]
    F --> G[Apply Thresholds]
    G --> H[Final Metric<br>Evaluated/Total Resources %]

    graph TD
    A[QER Dataset] --> B[Get Total Resources]
    C[Non-Compliant History] --> D[Count Active NC Resources]
    E[Closed NC Resources] --> D
    B --> F[Calculate Compliance]
    D --> F
    F --> G[Apply Thresholds]
    G --> H[Final Metric<br>Compliant/Total Resources %]

    graph TD
    A[TCRD Dataset] --> B[Get Non-Compliant Resources]
    C[Closed NC Resources] --> B
    D[QER Dataset] --> E[Get Total Resources]
    B --> F[Identify Past SLA<br>Based on Risk Level]
    E --> G[Calculate Past SLA %]
    F --> G
    G --> H[Apply Thresholds]
    H --> I[Final Metric<br>100% if no past SLA<br>else Past SLA %]

    graph TD
    A[Source Datasets] --> B[Extract Detailed Data]
    B --> C[Join & Group Data]
    C --> D[Calculate Metrics]
    D --> E[Present Results By:<br>Account/Region/ASV]
    
    subgraph "Data Sources"
    A1[QER Dataset]
    A2[Non-Compliant History]
    A3[TCRD Dataset]
    end
    
    subgraph "Key Metrics"
    M1[Resource Counts]
    M2[Compliance Status]
    M3[SLA Status]
    M4[Aging Information]
    end
