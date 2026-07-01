# Relationships

## Safe Flat Relationship Views

None discovered.

## Bridge Views

### IDG_ESTIMATE_QAD → IDG_SARASOTA_ESTIMATE_LINE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 1.0000 |
| Cardinality | N:M |
| View | `V_BRIDGE_ESTIMATE_TO_SARASOTA_ESTIMATE_LINE_BY_ESTIMATE_NUMBER_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 0.9965 |
| Cardinality | N:M |
| View | `V_BRIDGE_ORDER_TO_INVOICE_BY_ESTIMATE_NUMBER_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `PO_NUMBER` |
| To Column | `PO_NUMBER` |
| Confidence | MEDIUM |
| Match Rate | 0.9569 |
| Cardinality | N:M |
| View | `V_BRIDGE_ORDER_TO_INVOICE_BY_PO_NUMBER_QAD` |

## Summary Views

### IDG_ESTIMATE_QAD → IDG_SARASOTA_ESTIMATE_LINE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 1.0000 |
| Cardinality | N:M |
| View | `V_SUMMARY_ESTIMATE_TO_SARASOTA_ESTIMATE_LINE_BY_ESTIMATE_NUMBER_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 0.9965 |
| Cardinality | N:M |
| View | `V_SUMMARY_ORDER_TO_INVOICE_BY_ESTIMATE_NUMBER_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `PO_NUMBER` |
| To Column | `PO_NUMBER` |
| Confidence | MEDIUM |
| Match Rate | 0.9569 |
| Cardinality | N:M |
| View | `V_SUMMARY_ORDER_TO_INVOICE_BY_PO_NUMBER_QAD` |

## Skipped Unsafe Relationships

### IDG_ESTIMATE_QAD → IDG_SARASOTA_ESTIMATE_LINE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 1.0000 |
| Cardinality | N:M |
| Skip Reason | N:M relationship is unsafe for flat joined view |
| Unsafe Flat View | `V_ESTIMATE_WITH_SARASOTA_ESTIMATE_LINE_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `ESTIMATE_NUMBER` |
| To Column | `ESTIMATE_NUMBER` |
| Confidence | HIGH |
| Match Rate | 0.9965 |
| Cardinality | N:M |
| Skip Reason | N:M relationship is unsafe for flat joined view |
| Unsafe Flat View | `V_ORDER_WITH_INVOICE_QAD` |

### IDG_ORDER_QAD → IDG_INVOICE_QAD

| Field | Value |
|---|---|
| From Column | `PO_NUMBER` |
| To Column | `PO_NUMBER` |
| Confidence | MEDIUM |
| Match Rate | 0.9569 |
| Cardinality | N:M |
| Skip Reason | N:M relationship is unsafe for flat joined view |
| Unsafe Flat View | `V_ORDER_WITH_INVOICE_QAD` |

