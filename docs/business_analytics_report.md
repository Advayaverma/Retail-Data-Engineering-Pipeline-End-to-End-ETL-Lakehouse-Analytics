# Executive Retail Analytics & Business Insights Report

**Data Warehouse:** Kimball Star Schema (`fact_sales`, `dim_date`, `dim_customer`, `dim_product`, `dim_store`)  
**Data Volume:** 104,110 line items  
**Target Role:** Celebal Technologies Data Engineer Evaluation

---

## 1. Executive Summary & Core Business Metrics

| Business Metric | Value | Description |
| :--- | :--- | :--- |
| **Total Active Customers** | 10,500 | Distinct registered customers with purchases |
| **Repeat Customer Rate** | **99.96%** | Customers with $>1$ historical order |
| **One-Time Buyers** | 4 | Customers with exactly 1 order |
| **Repeat Buyers** | 10,496 | Loyal recurring customers |

---

## 2. Regional Performance & Market Share Breakdown

| Region | Store Count | Net Revenue | Net Profit | Profit Margin % | Revenue Share % |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **South** | 18 | $24,343,854.49 | $5,657,291.35 | 23.24% | 31.94% |
| **West** | 16 | $22,265,904.14 | $5,191,491.94 | 23.32% | 29.21% |
| **East** | 13 | $18,109,475.10 | $4,192,460.60 | 23.15% | 23.76% |
| **Central** | 8 | $11,503,874.29 | $2,685,057.54 | 23.34% | 15.09% |

---

## 3. Customer Lifetime Value (CLV) by Segment

| Customer Segment | Customer Count | Avg Orders / Customer | Average CLV | Total Segment Revenue |
| :--- | :--- | :--- | :--- | :--- |
| **Corporate** | 498 | 9.87 | **$7,406.37** | $3,688,373.78 |
| **VIP** | 1,014 | 9.92 | **$7,357.97** | $7,460,978.21 |
| **Standard** | 6,158 | 9.92 | **$7,264.35** | $44,733,884.52 |
| **Premium** | 2,671 | 9.79 | **$7,084.65** | $18,923,089.31 |

---

## 4. Top 10 Best-Selling Products by Revenue

| Product Name | Category | Brand | Units Sold | Net Revenue | Profit | Margin % |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| Dell Computers Model 172 | Electronics | Dell | 251 | **$712,741.05** | $203,271.29 | 28.52% |
| HP Computers Model 259 | Electronics | HP | 270 | **$642,702.81** | $155,652.51 | 24.22% |
| Dell Computers Model 461 | Electronics | Dell | 207 | **$623,084.16** | $168,870.27 | 27.1% |
| Apple Computers Model 987 | Electronics | Apple | 206 | **$610,722.26** | $187,643.58 | 30.72% |
| HP Computers Model 510 | Electronics | HP | 218 | **$575,935.43** | $173,112.85 | 30.06% |
| Apple Computers Model 202 | Electronics | Apple | 203 | **$527,911.32** | $160,562.52 | 30.41% |
| Apple Computers Model 193 | Electronics | Apple | 207 | **$509,426.15** | $67,226.54 | 13.2% |
| Asus Computers Model 529 | Electronics | Asus | 194 | **$496,083.24** | $118,312.86 | 23.85% |
| HP Computers Model 344 | Electronics | HP | 196 | **$494,318.23** | $119,838.67 | 24.24% |
| Apple Computers Model 812 | Electronics | Apple | 179 | **$491,792.48** | $143,052.15 | 29.09% |
