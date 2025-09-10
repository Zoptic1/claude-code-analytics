from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from data_processor import DataProcessor
import os
import json
from datetime import datetime
from werkzeug.utils import secure_filename
import shutil

app = Flask(__name__)
CORS(app)

# Configuration
UPLOAD_FOLDER = 'data'
ALLOWED_EXTENSIONS = {'csv'}
DATA_FILE = os.path.join('..', UPLOAD_FOLDER, 'AI_Note_Taking_Pin_Sales_Data__2025_.csv')

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize data processor
processor = DataProcessor(DATA_FILE)

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({'status': 'healthy', 'timestamp': datetime.now().isoformat()})

@app.route('/api/kpi')
def get_kpi():
    """Get key performance indicators"""
    try:
        metrics = processor.get_kpi_metrics()
        return jsonify(metrics)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/revenue-by-region')
def get_revenue_by_region():
    """Get revenue breakdown by region"""
    try:
        data = processor.get_revenue_by_region()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/revenue-by-channel')
def get_revenue_by_channel():
    """Get revenue breakdown by sales channel"""
    try:
        data = processor.get_revenue_by_channel()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/daily-trends')
def get_daily_trends():
    """Get daily revenue trends"""
    try:
        data = processor.get_daily_revenue_trend()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/monthly-trends')
def get_monthly_trends():
    """Get monthly trends"""
    try:
        data = processor.get_monthly_trends()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/price-distribution')
def get_price_distribution():
    """Get price distribution data"""
    try:
        data = processor.get_price_distribution()
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/filter-options')
def get_filter_options():
    """Get available filter options"""
    try:
        options = processor.get_filter_options()
        return jsonify(options)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/data')
def get_data():
    """Get paginated table data with optional filters"""
    try:
        # Get query parameters
        page = int(request.args.get('page', 1))
        per_page = int(request.args.get('per_page', 50))
        sort_by = request.args.get('sort_by', 'Date')
        sort_order = request.args.get('sort_order', 'desc')
        
        # Get filters from query parameters
        filters = {}
        if request.args.get('start_date'):
            filters['start_date'] = request.args.get('start_date')
        if request.args.get('end_date'):
            filters['end_date'] = request.args.get('end_date')
        if request.args.get('regions'):
            filters['regions'] = request.args.get('regions').split(',')
        if request.args.get('channels'):
            filters['channels'] = request.args.get('channels').split(',')
        if request.args.get('min_price'):
            filters['min_price'] = float(request.args.get('min_price'))
        if request.args.get('max_price'):
            filters['max_price'] = float(request.args.get('max_price'))
        
        # Apply filters if any
        if filters:
            # Create a temporary processor with filtered data
            filtered_df = processor.filter_data(filters)
            if filtered_df is not None:
                temp_processor = DataProcessor.__new__(DataProcessor)
                temp_processor.df = filtered_df
                temp_processor.csv_path = processor.csv_path
                data = temp_processor.get_table_data(page, per_page, sort_by, sort_order)
            else:
                data = {'data': [], 'total': 0, 'pages': 0, 'current_page': page}
        else:
            data = processor.get_table_data(page, per_page, sort_by, sort_order)
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/upload', methods=['POST'])
def upload_file():
    """Upload and replace CSV file"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'No file selected'}), 400
        
        if not allowed_file(file.filename):
            return jsonify({'error': 'Invalid file type. Only CSV files are allowed.'}), 400
        
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_path = os.path.join(app.config['UPLOAD_FOLDER'], f'temp_{filename}')
        file.save(temp_path)
        
        # Validate CSV structure
        try:
            test_processor = DataProcessor(temp_path)
            if test_processor.df is None:
                os.remove(temp_path)
                return jsonify({'error': 'Invalid CSV file format'}), 400
            
            # Check if it has the required columns
            required_columns = {'Sale_ID', 'Date', 'Units_Sold', 'Price_Per_Unit', 'Revenue', 'Region', 'Sales_Channel'}
            if not required_columns.issubset(set(test_processor.df.columns)):
                os.remove(temp_path)
                return jsonify({'error': f'CSV must contain columns: {", ".join(required_columns)}'}), 400
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.remove(temp_path)
            return jsonify({'error': f'Error validating CSV: {str(e)}'}), 400
        
        # Backup current file
        backup_path = os.path.join(app.config['UPLOAD_FOLDER'], f'backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        if os.path.exists(DATA_FILE):
            shutil.copy2(DATA_FILE, backup_path)
        
        # Replace the current file
        shutil.move(temp_path, DATA_FILE)
        
        # Reload data processor
        global processor
        processor = DataProcessor(DATA_FILE)
        
        return jsonify({
            'message': 'File uploaded successfully',
            'records': len(processor.df),
            'backup_file': os.path.basename(backup_path)
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/analytics/filtered', methods=['POST'])
def get_filtered_analytics():
    """Get analytics for filtered data"""
    try:
        filters = request.json or {}
        
        # Apply filters
        filtered_df = processor.filter_data(filters)
        if filtered_df is None:
            return jsonify({'error': 'Failed to apply filters'}), 400
        
        # Create temporary processor with filtered data
        temp_processor = DataProcessor.__new__(DataProcessor)
        temp_processor.df = filtered_df
        temp_processor.csv_path = processor.csv_path
        
        # Get all analytics for filtered data
        analytics = {
            'kpi': temp_processor.get_kpi_metrics(),
            'revenue_by_region': temp_processor.get_revenue_by_region(),
            'revenue_by_channel': temp_processor.get_revenue_by_channel(),
            'daily_trends': temp_processor.get_daily_revenue_trend(),
            'monthly_trends': temp_processor.get_monthly_trends(),
            'price_distribution': temp_processor.get_price_distribution()
        }
        
        return jsonify(analytics)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Serve static files for production
@app.route('/')
def serve_frontend():
    """Serve the React frontend"""
    return send_from_directory('../frontend/build', 'index.html')

@app.route('/static/<path:path>')
def serve_static(path):
    """Serve static assets"""
    return send_from_directory('../frontend/build/static', path)

if __name__ == '__main__':
    # Ensure data directory exists
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    
    print("🚀 Starting Analytics Dashboard Server...")
    print(f"📊 Data file: {DATA_FILE}")
    print(f"📈 Records loaded: {len(processor.df) if processor.df is not None else 0}")
    print("🌐 Server running at: http://localhost:5001")
    
    app.run(debug=True, host='0.0.0.0', port=5001)