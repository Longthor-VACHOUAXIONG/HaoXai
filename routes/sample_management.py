"""
Sample Management Routes
API endpoints for managing sample-virus relationships and sample data
"""
from flask import Blueprint, request, jsonify, session
from database.sample_manager import SampleManager
from database.db_manager import DatabaseManager
import traceback

sample_bp = Blueprint('sample_management', __name__, url_prefix='/sample')


def get_sample_manager():
    """Get a sample manager instance using session database connection"""
    try:
        # Get database connection info from session (same pattern as other routes)
        db_path = session.get('db_path')
        db_type = session.get('db_type', 'sqlite')
        
        if not db_path:
            raise ValueError("No database connection configured")
        
        return SampleManager(db_path, db_type)
    except Exception as e:
        print(f"[ERROR] Failed to create sample manager: {e}")
        raise


def check_db_connection():
    """Check if database connection is configured"""
    if not session.get('db_path'):
        return False, 'No database connection configured. Please connect to a database first.'
    return True, None


@sample_bp.route('/summary/<sample_id>', methods=['GET'])
def get_sample_summary(sample_id):
    """Get comprehensive summary for a sample"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        sample_manager = get_sample_manager()
        summary = sample_manager.get_sample_summary(sample_id)
        
        if summary:
            return jsonify({
                'success': True,
                'data': summary
            })
        else:
            return jsonify({
                'success': False,
                'error': f'Sample {sample_id} not found'
            }), 404
            
    except Exception as e:
        print(f"[ERROR] Failed to get sample summary: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/viruses/<sample_id>', methods=['GET'])
def get_sample_viruses(sample_id):
    """Get all virus types detected for a sample"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        sample_manager = get_sample_manager()
        viruses = sample_manager.get_sample_viruses(sample_id)
        
        return jsonify({
            'success': True,
            'data': viruses,
            'count': len(viruses)
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to get sample viruses: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/multiple-viruses', methods=['GET'])
def get_samples_with_multiple_viruses():
    """Get samples that have multiple virus types detected"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        sample_manager = get_sample_manager()
        samples = sample_manager.get_samples_with_multiple_viruses()
        
        return jsonify({
            'success': True,
            'data': samples,
            'count': len(samples)
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to get samples with multiple viruses: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/by-virus/<virus_type>', methods=['GET'])
def get_samples_by_virus(virus_type):
    """Get all samples that have a specific virus type"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        sample_manager = get_sample_manager()
        samples = sample_manager.search_samples_by_virus(virus_type)
        
        return jsonify({
            'success': True,
            'data': samples,
            'count': len(samples),
            'virus_type': virus_type
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to get samples by virus: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/register', methods=['POST'])
def register_sample_virus():
    """Manually register a virus type for a sample"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No data provided'
            }), 400
        
        sample_id = data.get('sample_id')
        virus_type = data.get('virus_type')
        notes = data.get('notes')
        
        if not sample_id or not virus_type:
            return jsonify({
                'success': False,
                'error': 'sample_id and virus_type are required'
            }), 400
        
        sample_manager = get_sample_manager()
        record_id = sample_manager.register_sample_virus(sample_id, virus_type, notes)
        
        return jsonify({
            'success': True,
            'data': {
                'record_id': record_id,
                'sample_id': sample_id,
                'virus_type': virus_type,
                'notes': notes
            },
            'message': f'Successfully registered {virus_type} for sample {sample_id}'
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to register sample virus: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/update-counts/<sample_id>', methods=['POST'])
def update_sequence_counts(sample_id):
    """Update sequence counts for a sample (call after sequence operations)"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        data = request.get_json() or {}
        virus_type = data.get('virus_type')  # Optional specific virus type
        
        sample_manager = get_sample_manager()
        sample_manager.update_sequence_counts(sample_id, virus_type)
        
        return jsonify({
            'success': True,
            'message': f'Sequence counts updated for sample {sample_id}'
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to update sequence counts: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/statistics', methods=['GET'])
def get_sample_statistics():
    """Get overall sample statistics"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        sample_manager = get_sample_manager()
        
        # Get samples with multiple viruses
        multi_virus_samples = sample_manager.get_samples_with_multiple_viruses()
        
        # Calculate statistics
        total_multi_virus = len(multi_virus_samples)
        total_sequences_multi = sum(sample['total_sequences'] for sample in multi_virus_samples)
        total_consensus_multi = sum(sample['total_consensus'] for sample in multi_virus_samples)
        
        # Group by virus count
        virus_count_distribution = {}
        for sample in multi_virus_samples:
            virus_count = sample['virus_count']
            virus_count_distribution[virus_count] = virus_count_distribution.get(virus_count, 0) + 1
        
        return jsonify({
            'success': True,
            'data': {
                'total_samples_with_multiple_viruses': total_multi_virus,
                'total_sequences_in_multi_virus_samples': total_sequences_multi,
                'total_consensus_in_multi_virus_samples': total_consensus_multi,
                'virus_count_distribution': virus_count_distribution,
                'samples': multi_virus_samples[:20]  # Return first 20 for preview
            }
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to get sample statistics: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@sample_bp.route('/search', methods=['POST'])
def search_samples():
    """Advanced sample search with multiple filters"""
    try:
        # Check database connection
        is_connected, error_msg = check_db_connection()
        if not is_connected:
            return jsonify({'success': False, 'error': error_msg}), 400
        
        data = request.get_json()
        
        if not data:
            return jsonify({
                'success': False,
                'error': 'No search criteria provided'
            }), 400
        
        sample_id_pattern = data.get('sample_id_pattern')
        virus_types = data.get('virus_types', [])
        min_virus_count = data.get('min_virus_count')
        max_virus_count = data.get('max_virus_count')
        
        sample_manager = get_sample_manager()
        
        # For now, implement basic search by virus type
        # This can be extended with more complex queries
        results = []
        
        if virus_types:
            # Get samples for each virus type
            for virus_type in virus_types:
                samples = sample_manager.search_samples_by_virus(virus_type)
                for sample in samples:
                    sample_info = sample_manager.get_sample_summary(sample)
                    if sample_info:
                        results.append(sample_info)
        
        # Remove duplicates (samples with multiple viruses)
        unique_results = []
        seen_samples = set()
        for result in results:
            sample_id = result.get('sample_id')
            if sample_id and sample_id not in seen_samples:
                unique_results.append(result)
                seen_samples.add(sample_id)
        
        # Apply additional filters
        if min_virus_count:
            unique_results = [r for r in unique_results if len(r.get('viruses', [])) >= min_virus_count]
        
        if max_virus_count:
            unique_results = [r for r in unique_results if len(r.get('viruses', [])) <= max_virus_count]
        
        if sample_id_pattern:
            import re
            pattern = re.compile(sample_id_pattern, re.IGNORECASE)
            unique_results = [r for r in unique_results if pattern.search(r.get('sample_id', ''))]
        
        return jsonify({
            'success': True,
            'data': unique_results,
            'count': len(unique_results),
            'search_criteria': data
        })
        
    except Exception as e:
        print(f"[ERROR] Failed to search samples: {e}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500