"""
Data Linking Routes
Handles creating and viewing relationships between specimen data, screening, and sequences
"""
from flask import Blueprint, request, jsonify, session, render_template
from database.sequence_db import SequenceDBManager
# from database.screening_host_storage_db import ScreeningHostStorageManager  # Temporarily disabled
from functools import wraps

linking_bp = Blueprint('linking', __name__)


def require_db_connection(f):
    """Decorator to ensure database connection exists"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('db_connected'):
            return jsonify({'success': False, 'message': 'Not connected to database'}), 401
        return f(*args, **kwargs)
    return decorated_function


def get_db_connection():
    """Get database connection from session"""
    db_type = session.get('db_type')
    if db_type == 'sqlite':
        return session.get('db_path'), db_type
    else:
        return session.get('db_params'), db_type


@linking_bp.route('/link')
@require_db_connection
def linking_page():
    """Data linking interface"""
    return render_template('database/linking.html')


@linking_bp.route('/link/bat-screening', methods=['POST'])
@require_db_connection
def link_bat_screening():
    """Link bat specimen to screening sample"""
    try:
        data = request.get_json()
        bat_data_id = data.get('bat_data_id')
        screening_data_id = data.get('screening_data_id')
        source_id = data.get('source_id')
        notes = data.get('notes')
        
        if not bat_data_id or not screening_data_id:
            return jsonify({'success': False, 'message': 'bat_data_id and screening_data_id are required'}), 400
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        link_id = db_manager.link_bat_to_screening(bat_data_id, screening_data_id, source_id, notes)
        
        return jsonify({
            'success': True,
            'link_id': link_id,
            'message': 'Bat linked to screening sample successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/link/market-screening', methods=['POST'])
@require_db_connection
def link_market_screening():
    """Link market specimen to screening sample"""
    try:
        data = request.get_json()
        market_data_id = data.get('market_data_id')
        screening_data_id = data.get('screening_data_id')
        source_id = data.get('source_id')
        notes = data.get('notes')
        
        if not market_data_id or not screening_data_id:
            return jsonify({'success': False, 'message': 'market_data_id and screening_data_id are required'}), 400
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        link_id = db_manager.link_market_to_screening(market_data_id, screening_data_id, source_id, notes)
        
        return jsonify({
            'success': True,
            'link_id': link_id,
            'message': 'Market specimen linked to screening sample successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/link/rodent-screening', methods=['POST'])
@require_db_connection
def link_rodent_screening():
    """Link rodent host to screening sample"""
    try:
        data = request.get_json()
        rodent_host_id = data.get('rodent_host_id')
        screening_data_id = data.get('screening_data_id')
        field_id = data.get('field_id')
        source_id = data.get('source_id')
        notes = data.get('notes')
        
        if not rodent_host_id or not screening_data_id:
            return jsonify({'success': False, 'message': 'rodent_host_id and screening_data_id are required'}), 400
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        link_id = db_manager.link_rodent_to_screening(rodent_host_id, screening_data_id, field_id, source_id, notes)
        
        return jsonify({
            'success': True,
            'link_id': link_id,
            'message': 'Rodent host linked to screening sample successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/link/screening-sequence', methods=['POST'])
@require_db_connection
def link_screening_sequence():
    """Link screening sample to sequence"""
    try:
        data = request.get_json()
        screening_data_id = data.get('screening_data_id')
        sample_id = data.get('sample_id')
        sequence_id = data.get('sequence_id')
        consensus_id = data.get('consensus_id')
        blast_result_id = data.get('blast_result_id')
        virus_type_matched = data.get('virus_type_matched')
        sequence_confirmed = data.get('sequence_confirmed', False)
        notes = data.get('notes')
        
        if not screening_data_id or not sample_id:
            return jsonify({'success': False, 'message': 'screening_data_id and sample_id are required'}), 400
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        link_id = db_manager.link_screening_to_sequence(
            screening_data_id, sample_id, sequence_id, consensus_id, 
            blast_result_id, virus_type_matched, sequence_confirmed, notes
        )
        
        return jsonify({
            'success': True,
            'link_id': link_id,
            'message': 'Screening linked to sequence successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/link/freezer-sample', methods=['POST'])
@require_db_connection
def link_freezer_sample():
    """Link freezer storage to sample"""
    try:
        data = request.get_json()
        freezer_storage_id = data.get('freezer_storage_id')
        sample_type = data.get('sample_type')  # swab, tissue, environmental, market, rodentsample
        sample_id_value = data.get('sample_id_value')
        sample_id_identifier = data.get('sample_id_identifier')
        notes = data.get('notes')
        
        if not freezer_storage_id or not sample_type or not sample_id_value:
            return jsonify({'success': False, 'message': 'freezer_storage_id, sample_type, and sample_id_value are required'}), 400
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        link_id = db_manager.link_freezer_to_sample(
            freezer_storage_id, sample_type, sample_id_value, sample_id_identifier, notes
        )
        
        return jsonify({
            'success': True,
            'link_id': link_id,
            'message': f'Freezer storage linked to {sample_type} sample successfully'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== RETRIEVAL ROUTES ====================

@linking_bp.route('/get/bat-screenings/<int:bat_data_id>', methods=['GET'])
@require_db_connection
def get_bat_screenings(bat_data_id):
    """Get all screening samples for a bat"""
    try:
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        results = db_manager.get_bat_screenings(bat_data_id)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/get/market-screenings/<int:market_data_id>', methods=['GET'])
@require_db_connection
def get_market_screenings(market_data_id):
    """Get all screening samples for a market specimen"""
    try:
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        results = db_manager.get_market_screenings(market_data_id)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/get/rodent-screenings/<int:rodent_host_id>', methods=['GET'])
@require_db_connection
def get_rodent_screenings(rodent_host_id):
    """Get all screening samples for a rodent"""
    try:
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        results = db_manager.get_rodent_screenings(rodent_host_id)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/get/screening-sequences/<int:screening_data_id>', methods=['GET'])
@require_db_connection
def get_screening_sequences(screening_data_id):
    """Get all sequences for a screening sample"""
    try:
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        results = db_manager.get_screening_sequences(screening_data_id)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@linking_bp.route('/get/freezer-samples/<int:freezer_storage_id>', methods=['GET'])
@require_db_connection
def get_freezer_samples(freezer_storage_id):
    """Get all samples in freezer storage"""
    try:
        sample_type = request.args.get('sample_type')  # Optional filter
        
        db_conn, db_type = get_db_connection()
        db_manager = SequenceDBManager(db_conn, db_type)
        
        results = db_manager.get_freezer_samples(freezer_storage_id, sample_type)
        
        return jsonify({
            'success': True,
            'count': len(results),
            'data': results
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


# ==================== SEARCH AND QUERY ROUTES ====================

@linking_bp.route('/search/records', methods=['POST'])
@require_db_connection
def search_records():
    """Search for records in a table by identifier"""
    try:
        return jsonify({'success': False, 'message': 'Search function temporarily disabled'}), 503
        # Temporarily disabled - file corruption issue
        # data = request.get_json()
        # table_name = data.get('table_name')
        # search_column = data.get('search_column')
        # search_value = data.get('search_value')
        # 
        # if not table_name or not search_column or not search_value:
        #     return jsonify({'success': False, 'message': 'table_name, search_column, and search_value are required'}), 400
        # 
        # db_conn, db_type = get_db_connection()
        # db_manager = ScreeningHostStorageManager(db_conn, db_type)
        # 
        # # Get records
        # results = db_manager.search_records(table_name, search_column, search_value)
        # 
        # return jsonify({
        #     'success': True,
        #     'count': len(results),
        #     'data': results
        # })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500
