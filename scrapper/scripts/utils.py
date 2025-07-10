from datetime import datetime, timedelta

def generate_dates(start_date, end_date, delta):
    current_date = start_date
    while current_date <= end_date:
        yield current_date.strftime('%Y-%m-%d')
        current_date += delta