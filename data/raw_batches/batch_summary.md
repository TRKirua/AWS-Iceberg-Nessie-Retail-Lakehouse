# Batch Data Summary

This directory contains batch CSV files generated from `superstore_sales.csv`
with controlled data quality issues for testing the Silver layer.

## Quality Issues Introduced (~5% of rows)

- **Null Sales**: Missing sales values
- **Null Product ID**: Missing product identifiers
- **Invalid Discount**: Discount values > 1 or < 0
- **Invalid Quantity**: Quantity <= 0
- **Duplicates**: Duplicate order_id + product_id combinations (~1%)

## Expected Filtering Behavior

| Layer  | Expected Action                          |
|--------|------------------------------------------|
| Bronze | Keep all records (append-only)           |
| Silver | Filter nulls, duplicates, invalid values |
| Gold   | Aggregate clean Silver data              |

## Batch Files

### sales_batch_001.csv
- Records: 3,364
- Null sales: 38
- Null Product ID: 31
- Invalid discount: 33
- Invalid quantity: 36

### sales_batch_002.csv
- Records: 3,501
- Null sales: 137
- Null Product ID: 137
- Invalid discount: 43
- Invalid quantity: 32

### sales_batch_003.csv
- Records: 3,500
- Null sales: 135
- Null Product ID: 135
- Invalid discount: 49
- Invalid quantity: 30

