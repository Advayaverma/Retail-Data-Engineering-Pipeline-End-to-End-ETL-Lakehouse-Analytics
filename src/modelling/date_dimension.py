"""
Date Dimension Generator for Kimball Star Schema.
Generates comprehensive calendar and fiscal attributes with integer surrogate keys (YYYYMMDD).
"""

from datetime import datetime, timedelta


def generate_date_dimension(start_date_str: str = "2021-01-01", end_date_str: str = "2026-12-31") -> list:
    """
    Generate all calendar records between start_date and end_date.
    
    Returns:
        List of dictionaries with date dimension attributes.
    """
    start_dt = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_dt = datetime.strptime(end_date_str, "%Y-%m-%d")
    delta_days = (end_dt - start_dt).days + 1

    date_rows = []
    for i in range(delta_days):
        current_dt = start_dt + timedelta(days=i)
        
        # Surrogate key: integer YYYYMMDD
        date_sk = int(current_dt.strftime("%Y%m%d"))
        full_date = current_dt.strftime("%Y-%m-%d")
        
        # Day attributes
        day_of_week = current_dt.isoweekday()  # 1 = Monday, 7 = Sunday
        day_name = current_dt.strftime("%A")
        day_of_month = current_dt.day
        day_of_year = int(current_dt.strftime("%j"))
        week_of_year = int(current_dt.strftime("%V"))
        is_weekend = day_of_week in (6, 7)

        # Month attributes
        month_num = current_dt.month
        month_name = current_dt.strftime("%B")

        # Quarter attributes
        quarter_num = (month_num - 1) // 3 + 1
        quarter_name = f"Q{quarter_num}"
        year_num = current_dt.year

        # Fiscal attributes (Retail fiscal year starting April 1st)
        if month_num >= 4:
            fiscal_quarter = f"FQ{(month_num - 4) // 3 + 1}"
            fiscal_year = year_num + 1
        else:
            fiscal_quarter = f"FQ{(month_num + 8) // 3 + 1}"
            fiscal_year = year_num

        date_rows.append({
            "date_sk": date_sk,
            "full_date": full_date,
            "day_of_week": day_of_week,
            "day_name": day_name,
            "day_of_month": day_of_month,
            "day_of_year": day_of_year,
            "week_of_year": week_of_year,
            "month_num": month_num,
            "month_name": month_name,
            "quarter_num": quarter_num,
            "quarter_name": quarter_name,
            "year_num": year_num,
            "is_weekend": is_weekend,
            "fiscal_quarter": fiscal_quarter,
            "fiscal_year": fiscal_year,
        })

    return date_rows
