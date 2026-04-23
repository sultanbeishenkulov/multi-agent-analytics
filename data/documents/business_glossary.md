# Business Glossary

## Revenue
Revenue is calculated as: `quantity × unit_price × (1 - discount_pct / 100)`.
Only orders with `status = 'completed'` count toward revenue metrics unless explicitly stated otherwise.
Returned and cancelled orders are excluded from all revenue KPIs.

## Customer Segments
- **Enterprise**: Large organizations (500+ employees), high contract values, longer sales cycles.
- **SMB**: Small and medium businesses (10–499 employees), mid-tier contract values.
- **Consumer**: Individual users or very small teams (<10 employees), self-serve, lowest ACV.

## Lifetime Value (LTV)
Stored in `customers.lifetime_value`. Updated nightly. Represents total completed-order revenue for a customer since signup.

## Cohort
A group of customers who signed up in the same calendar month. Used for retention and LTV analysis.

## Churn Signal
A customer is considered at churn risk if their last completed order was more than 6 months ago.

## Campaign ROI
ROI = total revenue attributed to campaign / campaign budget. A ratio above 1.0 means the campaign paid for itself.

## Average Order Value (AOV)
AOV = total revenue / number of completed orders. Tracked per segment and region.

## Support Satisfaction (CSAT)
Measured on a 1–5 scale. A score below 3 is considered poor. Target is 4.0+.

## Resolution Time
Time from `created_at` to `resolved_at` on a support ticket, measured in hours.
