import polars as pl
from datetime import datetime
import json

class DataProcessor:
    def __init__(self, csv_path):
        self.csv_path = csv_path
        self.df = None
        self.load_data()
    
    def load_data(self):
        """Load CSV data using Polars with proper data types"""
        try:
            self.df = pl.read_csv(
                self.csv_path,
                try_parse_dates=True,
                dtypes={
                    'Sale_ID': pl.Int64,
                    'Date': pl.Date,
                    'Units_Sold': pl.Int64,
                    'Price_Per_Unit': pl.Float64,
                    'Revenue': pl.Float64,
                    'Region': pl.Utf8,
                    'Sales_Channel': pl.Utf8
                }
            )
            print(f"Loaded {len(self.df)} records successfully")
        except Exception as e:
            print(f"Error loading data: {e}")
            self.df = None
    
    def get_kpi_metrics(self):
        """Calculate key performance indicators"""
        if self.df is None:
            return {}
        
        metrics = {
            'total_revenue': float(self.df['Revenue'].sum()),
            'total_units': int(self.df['Units_Sold'].sum()),
            'avg_price': float(self.df['Price_Per_Unit'].mean()),
            'total_transactions': len(self.df),
            'avg_units_per_sale': float(self.df['Units_Sold'].mean()),
            'revenue_per_transaction': float(self.df['Revenue'].mean())
        }
        
        # Top performers
        top_region = self.df.group_by('Region').agg(
            pl.sum('Revenue').alias('total_revenue')
        ).sort('total_revenue', descending=True).limit(1)
        
        top_channel = self.df.group_by('Sales_Channel').agg(
            pl.sum('Revenue').alias('total_revenue')
        ).sort('total_revenue', descending=True).limit(1)
        
        if len(top_region) > 0:
            metrics['top_region'] = top_region['Region'][0]
            metrics['top_region_revenue'] = float(top_region['total_revenue'][0])
        
        if len(top_channel) > 0:
            metrics['top_channel'] = top_channel['Sales_Channel'][0]
            metrics['top_channel_revenue'] = float(top_channel['total_revenue'][0])
        
        return metrics
    
    def get_revenue_by_region(self):
        """Get revenue breakdown by region"""
        if self.df is None:
            return []
        
        result = self.df.group_by('Region').agg([
            pl.sum('Revenue').alias('total_revenue'),
            pl.sum('Units_Sold').alias('total_units'),
            pl.count().alias('transaction_count')
        ]).sort('total_revenue', descending=True)
        
        return result.to_dicts()
    
    def get_revenue_by_channel(self):
        """Get revenue breakdown by sales channel"""
        if self.df is None:
            return []
        
        result = self.df.group_by('Sales_Channel').agg([
            pl.sum('Revenue').alias('total_revenue'),
            pl.sum('Units_Sold').alias('total_units'),
            pl.count().alias('transaction_count')
        ]).sort('total_revenue', descending=True)
        
        return result.to_dicts()
    
    def get_daily_revenue_trend(self):
        """Get daily revenue trends"""
        if self.df is None:
            return []
        
        result = self.df.group_by('Date').agg([
            pl.sum('Revenue').alias('daily_revenue'),
            pl.sum('Units_Sold').alias('daily_units'),
            pl.count().alias('daily_transactions')
        ]).sort('Date')
        
        # Convert to format suitable for charts
        trends = []
        for row in result.to_dicts():
            trends.append({
                'date': row['Date'].strftime('%Y-%m-%d'),
                'revenue': float(row['daily_revenue']),
                'units': int(row['daily_units']),
                'transactions': int(row['daily_transactions'])
            })
        
        return trends
    
    def get_price_distribution(self):
        """Get price point distribution"""
        if self.df is None:
            return []
        
        result = self.df.group_by('Price_Per_Unit').agg([
            pl.sum('Revenue').alias('total_revenue'),
            pl.sum('Units_Sold').alias('total_units'),
            pl.count().alias('transaction_count')
        ]).sort('Price_Per_Unit')
        
        return result.to_dicts()
    
    def get_monthly_trends(self):
        """Get monthly aggregated trends"""
        if self.df is None:
            return []
        
        result = self.df.with_columns([
            pl.col('Date').dt.month().alias('month'),
            pl.col('Date').dt.year().alias('year')
        ]).group_by(['year', 'month']).agg([
            pl.sum('Revenue').alias('monthly_revenue'),
            pl.sum('Units_Sold').alias('monthly_units'),
            pl.count().alias('monthly_transactions'),
            pl.mean('Price_Per_Unit').alias('avg_price')
        ]).sort(['year', 'month'])
        
        trends = []
        for row in result.to_dicts():
            trends.append({
                'year_month': f"{row['year']}-{row['month']:02d}",
                'revenue': float(row['monthly_revenue']),
                'units': int(row['monthly_units']),
                'transactions': int(row['monthly_transactions']),
                'avg_price': float(row['avg_price'])
            })
        
        return trends
    
    def filter_data(self, filters):
        """Apply filters to the dataset"""
        if self.df is None:
            return None
        
        filtered_df = self.df
        
        # Date range filter
        if 'start_date' in filters and filters['start_date']:
            filtered_df = filtered_df.filter(pl.col('Date') >= filters['start_date'])
        
        if 'end_date' in filters and filters['end_date']:
            filtered_df = filtered_df.filter(pl.col('Date') <= filters['end_date'])
        
        # Region filter
        if 'regions' in filters and filters['regions']:
            filtered_df = filtered_df.filter(pl.col('Region').is_in(filters['regions']))
        
        # Sales channel filter
        if 'channels' in filters and filters['channels']:
            filtered_df = filtered_df.filter(pl.col('Sales_Channel').is_in(filters['channels']))
        
        # Price range filter
        if 'min_price' in filters and filters['min_price'] is not None:
            filtered_df = filtered_df.filter(pl.col('Price_Per_Unit') >= filters['min_price'])
        
        if 'max_price' in filters and filters['max_price'] is not None:
            filtered_df = filtered_df.filter(pl.col('Price_Per_Unit') <= filters['max_price'])
        
        return filtered_df
    
    def get_filter_options(self):
        """Get available filter options"""
        if self.df is None:
            return {}
        
        return {
            'regions': self.df['Region'].unique().to_list(),
            'channels': self.df['Sales_Channel'].unique().to_list(),
            'price_range': {
                'min': float(self.df['Price_Per_Unit'].min()),
                'max': float(self.df['Price_Per_Unit'].max())
            },
            'date_range': {
                'start': self.df['Date'].min().strftime('%Y-%m-%d'),
                'end': self.df['Date'].max().strftime('%Y-%m-%d')
            }
        }
    
    def get_table_data(self, page=1, per_page=50, sort_by='Date', sort_order='desc'):
        """Get paginated table data"""
        if self.df is None:
            return {'data': [], 'total': 0, 'pages': 0}
        
        # Sort data
        ascending = sort_order == 'asc'
        sorted_df = self.df.sort(sort_by, descending=not ascending)
        
        # Pagination
        total_records = len(sorted_df)
        total_pages = (total_records + per_page - 1) // per_page
        offset = (page - 1) * per_page
        
        page_data = sorted_df.slice(offset, per_page)
        
        # Convert to dict format
        records = []
        for row in page_data.to_dicts():
            record = dict(row)
            record['Date'] = record['Date'].strftime('%Y-%m-%d')
            records.append(record)
        
        return {
            'data': records,
            'total': total_records,
            'pages': total_pages,
            'current_page': page
        }