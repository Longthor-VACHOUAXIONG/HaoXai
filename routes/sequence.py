"""
Sequence analysis routes using Tracy or improved Python consensus
"""
from flask import Blueprint, request, jsonify, session, render_template, current_app, send_file
from werkzeug.utils import secure_filename
import os
import subprocess
import json
import uuid
import re
from Bio import SeqIO
from Bio.Seq import Seq
from Bio.Align import PairwiseAligner
from Bio.Blast import NCBIWWW, NCBIXML
from io import BytesIO
import datetime
import base64
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
import time
from threading import Lock
from database.sequence_db import SequenceDBManager
from database.db_manager_flask import DatabaseManagerFlask

sequence_bp = Blueprint('sequence', __name__)

# Global progress tracking for BLAST operations
blast_progress_data = {'completed': 0, 'total': 0, 'status': 'idle', 'cancelled': False}
progress_lock = Lock()

# Ambiguous base lookup
AMBIGUOUS_LOOKUP = {
    'A': ['A'], 'T': ['T'], 'G': ['G'], 'C': ['C'],
    'R': ['A', 'G'], 'Y': ['C', 'T'], 'K': ['G', 'T'], 'M': ['A', 'C'],
    'S': ['C', 'G'], 'W': ['A', 'T'], 'B': ['C', 'G', 'T'],
    'D': ['A', 'G', 'T'], 'H': ['A', 'C', 'T'], 'V': ['A', 'C', 'G'],
    'N': ['A', 'C', 'G', 'T'], '-': ['-']
}


def parse_sample_id_and_target(filename):
    """
    Parse filename to extract sample_id and target_sequence
    Examples:
        CANB_ANA24_001-Hanta -> (CANB_ANA24_001, Hanta)
        CANB_TISL24_445-FHanta -> (CANB_TISL24_445, Hanta)
        CANB_TISL24_445-RHanta -> (CANB_TISL24_445, Hanta)
        CANB_ANA24_001-Hanta_F -> (CANB_ANA24_001, Hanta)
        CANB_ANA_001 -> (CANB_ANA_001, None)
    """
    # Remove file extension
    base = filename.replace('.ab1', '').replace('.abi', '').replace('.scf', '').replace('.fasta', '').replace('.fa', '')
    
    # Check if there's a dash
    if '-' in base:
        parts = base.split('-', 1)  # Split only on first dash
        sample_id = parts[0].strip()
        target = parts[1].strip() if len(parts) > 1 else None
        
        # Remove F/R indicators and technical suffixes from target if present
        if target:
            # Remove F/R at the BEGINNING (like -FHanta -> Hanta, -RHanta -> Hanta)
            target = re.sub(r'^[FR]', '', target, flags=re.IGNORECASE)
            # Remove F/R at the END (like Hanta_F -> Hanta, Hanta-R -> Hanta)
            target = re.sub(r'[_-][FR]$', '', target, flags=re.IGNORECASE)
            # Remove technical suffixes like _A01_01_RapidSeq50...
            target = re.sub(r'_[A-Z]\d+.*$', '', target)
            # Clean up any remaining separators
            target = target.strip('_-')
        
        return sample_id, target if target else None
    else:
        # No dash, just return base as sample_id
        # But still remove technical suffixes
        sample_id = re.sub(r'_[A-Z]\d+.*$', '', base)
        return sample_id, None


@sequence_bp.route('/analysis')
def analysis():
    """Sequence analysis page"""
    return render_template('sequence/analysis.html')


def get_filename_direction(filename, pattern_config=None):
    """Detect forward/reverse from filename patterns"""
    filename_lower = filename.lower()
    
    # If custom pattern config provided, use it
    if pattern_config and pattern_config.get('type') == 'custom':
        fwd_pattern = pattern_config.get('forward_pattern', '-F').lower()
        rev_pattern = pattern_config.get('reverse_pattern', '-R').lower()
        
        if fwd_pattern in filename_lower:
            return 'Forward'
        if rev_pattern in filename_lower:
            return 'Reverse'
        return None
    
    # Handle fhanta pattern specifically
    if pattern_config and pattern_config.get('type') == 'fhanta':
        # Pattern: -FVirus or -RVirus (like -FHanta, -RHanta)
        if re.search(r'[_-]f[a-z]+', filename_lower):
            return 'Forward'
        if re.search(r'[_-]r[a-z]+', filename_lower):
            return 'Reverse'
        return None
    
    # Check for various forward patterns
    # Matches: -F, _F, -FHanta, -Fcorona, _F_, -F., etc.
    if re.search(r'[_-]f(?:[a-z]*[_\-\.]|[a-z]+|(?![a-z]))', filename_lower):
        return 'Forward'
    
    # Check for full word "forward" or "fwd"
    if re.search(r'\bforward\b|\bfwd\b', filename_lower):
        return 'Forward'
    
    # Check for various reverse patterns
    # Matches: -R, _R, -RHanta, -Rcorona, _R_, -R., etc.
    if re.search(r'[_-]r(?:[a-z]*[_\-\.]|[a-z]+|(?![a-z]))', filename_lower):
        return 'Reverse'
    
    # Check for full word "reverse" or "rev"
    if re.search(r'\breverse\b|\brev\b', filename_lower):
        return 'Reverse'
    
    return None


def detect_sequence_direction(record, filename, pattern_config=None):
    """Detect actual sequence direction from AB1 file metadata"""
    try:
        # Method 1: Check mobility file tag (some sequencers mark this)
        if hasattr(record, 'annotations') and 'abif_raw' in record.annotations:
            abif_raw = record.annotations['abif_raw']
            
            # Check PCON2 tag (edited base positions - sometimes has direction info)
            # Check SMPL tag (sample name - might contain F/R)
            sample_name = abif_raw.get('SMPL', b'').decode('ascii', errors='ignore') if isinstance(abif_raw.get('SMPL', b''), bytes) else str(abif_raw.get('SMPL', ''))
            if sample_name:
                sample_lower = sample_name.lower()
                if 'forward' in sample_lower or '_f' in sample_lower or '-f' in sample_lower:
                    return 'Forward'
                if 'reverse' in sample_lower or '_r' in sample_lower or '-r' in sample_lower:
                    return 'Reverse'
        
        # Method 2: Use filename as fallback
        filename_dir = get_filename_direction(filename, pattern_config)
        if filename_dir:
            return filename_dir
        
        # Method 3: If no clear indicator, mark as unknown
        return None
        
    except Exception as e:
        print(f"Error detecting direction: {e}")
        return None


def check_sequence_complementarity(seq1, seq2, sample_size=100):
    """
    Check if two sequences are reverse complements of each other.
    Returns (complementarity_score, are_likely_reverse_complements)
    """
    try:
        # Take samples from both ends and middle
        seq1_str = str(seq1).upper()
        seq2_str = str(seq2).upper()
        
        # Get reverse complement of seq2
        seq2_rc = str(Seq(seq2_str).reverse_complement())
        
        # Calculate alignment score between seq1 and seq2 (as is)
        matches_direct = sum(1 for a, b in zip(seq1_str[:sample_size], seq2_str[:sample_size]) if a == b)
        
        # Calculate alignment score between seq1 and reverse complement of seq2
        matches_rc = sum(1 for a, b in zip(seq1_str[:sample_size], seq2_rc[:sample_size]) if a == b)
        
        # If reverse complement matches much better, they are F/R pair
        complementarity_score = matches_rc / sample_size if sample_size > 0 else 0
        direct_score = matches_direct / sample_size if sample_size > 0 else 0
        
        # Consider them reverse complements if RC match is significantly better
        are_reverse_complements = complementarity_score > 0.7 and complementarity_score > direct_score * 1.5
        
        return complementarity_score, direct_score, are_reverse_complements
        
    except Exception as e:
        print(f"Error checking complementarity: {e}")
        return 0, 0, False


def analyze_fr_pairs(sequences_info):
    """
    Analyze F/R pairs to detect if they are swapped based on sequence complementarity
    """
    # Group by base name
    from collections import defaultdict
    groups = defaultdict(list)
    
    for seq in sequences_info:
        groups[seq['group']].append(seq)
    
    # Check each group for F/R pairs
    for group_name, group_seqs in groups.items():
        if len(group_seqs) == 2:
            seq1, seq2 = group_seqs[0], group_seqs[1]
            
            # Check if one is labeled F and other is R
            dir1 = seq1.get('filename_direction', 'Unknown')
            dir2 = seq2.get('filename_direction', 'Unknown')
            
            if (dir1 == 'Forward' and dir2 == 'Reverse') or (dir1 == 'Reverse' and dir2 == 'Forward'):
                # Check complementarity
                comp_score, direct_score, are_rc = check_sequence_complementarity(
                    seq1.get('sequence', ''), 
                    seq2.get('sequence', '')
                )
                
                # If they DON'T match as reverse complements, they might be swapped
                if not are_rc and direct_score > comp_score:
                    # Sequences match better directly than as RC - likely swapped!
                    swap_warning = f"⚠️ LIKELY SWAPPED! Forward and Reverse sequences match directly ({direct_score*100:.1f}%) instead of as complements ({comp_score*100:.1f}%). Check labeling!"
                    
                    seq1['swap_warning'] = swap_warning
                    seq2['swap_warning'] = swap_warning
                    seq1['likely_swapped'] = True
                    seq2['likely_swapped'] = True
                    seq1['complementarity_score'] = comp_score
                    seq2['complementarity_score'] = comp_score
                    seq1['direct_match_score'] = direct_score
                    seq2['direct_match_score'] = direct_score
                elif are_rc:
                    # Good match as reverse complements
                    seq1['swap_warning'] = f"✓ Correct F/R pair (complementarity: {comp_score*100:.1f}%)"
                    seq2['swap_warning'] = f"✓ Correct F/R pair (complementarity: {comp_score*100:.1f}%)"
                    seq1['likely_swapped'] = False
                    seq2['likely_swapped'] = False
                    seq1['complementarity_score'] = comp_score
                    seq2['complementarity_score'] = comp_score
    
    return sequences_info


def detect_fr_pattern(filenames):
    """Automatically detect the F/R naming pattern from uploaded filenames"""
    import re
    
    # Patterns to detect
    patterns = {
        'virus_suffix': {
            'forward': r'[_-][a-zA-Z]+[_-]F(?![a-zA-Z])',
            'reverse': r'[_-][a-zA-Z]+[_-]R(?![a-zA-Z])',
            'name': 'Virus name before F/R (e.g., -hanta_F/-hanta_R)'
        },
        'fhanta': {
            'forward': r'[_-]F[a-zA-Z]+',
            'reverse': r'[_-]R[a-zA-Z]+',
            'name': 'With virus name (e.g., -FHanta/-RHanta)'
        },
        'standard_dash': {
            'forward': r'[_-]F(?![a-zA-Z])',
            'reverse': r'[_-]R(?![a-zA-Z])',
            'name': 'Standard with dash (e.g., -F/-R)'
        },
        'standard_underscore': {
            'forward': r'_F(?![a-zA-Z])',
            'reverse': r'_R(?![a-zA-Z])',
            'name': 'Standard with underscore (e.g., _F/_R)'
        }
    }
    
    # Count matches for each pattern
    pattern_scores = {}
    for pattern_name, pattern_info in patterns.items():
        fwd_count = sum(1 for f in filenames if re.search(pattern_info['forward'], f, re.IGNORECASE))
        rev_count = sum(1 for f in filenames if re.search(pattern_info['reverse'], f, re.IGNORECASE))
        pattern_scores[pattern_name] = {
            'score': fwd_count + rev_count,
            'forward_count': fwd_count,
            'reverse_count': rev_count,
            'name': pattern_info['name']
        }
    
    # Find best matching pattern
    best_pattern = max(pattern_scores.items(), key=lambda x: x[1]['score'])
    
    if best_pattern[1]['score'] > 0:
        return {
            'detected': True,
            'pattern_type': best_pattern[0],
            'pattern_name': best_pattern[1]['name'],
            'forward_count': best_pattern[1]['forward_count'],
            'reverse_count': best_pattern[1]['reverse_count'],
            'total_files': len(filenames)
        }
    
    return {
        'detected': False,
        'pattern_type': 'standard',
        'pattern_name': 'Standard (default)',
        'forward_count': 0,
        'reverse_count': 0,
        'total_files': len(filenames)
    }


@sequence_bp.route('/upload', methods=['POST'])
def upload_sequences():
    """Upload multiple AB1 files and generate consensus (PEARL-style)"""
    try:
        # Get files from request
        files = request.files.getlist('files[]')
        reference_file = request.files.get('reference')
        virus_type = request.form.get('virus_type', 'Other')
        
        if not files or files[0].filename == '':
            return jsonify({'success': False, 'message': 'No files selected'}), 400
        
        # Auto-detect pattern from filenames
        filenames = [f.filename for f in files if f.filename]
        detection_result = detect_fr_pattern(filenames)
        
        # Get F/R pattern configuration
        fr_pattern_type = request.form.get('fr_pattern_type', 'auto')
        
        # If auto-detection or standard, use detected pattern
        if fr_pattern_type == 'auto' or fr_pattern_type == 'standard':
            if detection_result['detected']:
                if detection_result['pattern_type'] == 'virus_suffix':
                    fr_pattern_type = 'virus_suffix'
                elif detection_result['pattern_type'] == 'fhanta':
                    fr_pattern_type = 'fhanta'
                else:
                    fr_pattern_type = 'standard'
            else:
                fr_pattern_type = 'standard'
        
        pattern_config = {
            'type': fr_pattern_type,
            'forward_pattern': None,
            'reverse_pattern': None,
            'suffix_pattern': None
        }
        
        if fr_pattern_type == 'custom':
            pattern_config['forward_pattern'] = request.form.get('forward_pattern', '-F')
            pattern_config['reverse_pattern'] = request.form.get('reverse_pattern', '-R')
            pattern_config['suffix_pattern'] = request.form.get('suffix_pattern', r'_[A-Z]\d+.*$')
        elif fr_pattern_type == 'virus_suffix':
            # Preset for -hanta_F/-hanta_R pattern (virus name before F/R)
            pattern_config['forward_pattern'] = 'virus_suffix_F'
            pattern_config['reverse_pattern'] = 'virus_suffix_R'
            pattern_config['suffix_pattern'] = r'_[A-Z]\d+.*$'
        elif fr_pattern_type == 'fhanta':
            # Preset for -FHanta/-RHanta pattern
            pattern_config['forward_pattern'] = '-F[a-zA-Z]+'
            pattern_config['reverse_pattern'] = '-R[a-zA-Z]+'
            pattern_config['suffix_pattern'] = r'_[A-Z]\d+.*$'
        else:  # standard
            pattern_config['forward_pattern'] = '-F'
            pattern_config['reverse_pattern'] = '-R'
            pattern_config['suffix_pattern'] = r'_[A-Z]\d+.*$'
        
        # Store in session for consistent use
        session['fr_pattern_config'] = pattern_config
        session['pattern_detection'] = detection_result
        
        # Debug logging
        print(f"\n=== Pattern Detection Debug ===")
        print(f"Detection result: {detection_result}")
        print(f"Selected pattern type: {pattern_config['type']}")
        print(f"Forward pattern: {pattern_config['forward_pattern']}")
        print(f"Reverse pattern: {pattern_config['reverse_pattern']}")
        print(f"================================\n")
        
        upload_folder = current_app.config['UPLOAD_FOLDER']
        session_id = str(uuid.uuid4())
        session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
        os.makedirs(session_folder, exist_ok=True)
        
        # Save AB1 files and group by base name
        from collections import defaultdict
        import re
        
        ab1_groups = defaultdict(list)  # Group files by base name
        sequences_info = []
        
        for file in files:
            if file and (file.filename.endswith('.ab1') or file.filename.endswith('.abi') or file.filename.endswith('.scf')):
                filename = secure_filename(file.filename)
                filepath = os.path.join(session_folder, filename)
                file.save(filepath)
                
                # Extract base name (remove F/R letters but preserve virus name like "Hanta")
                # Use custom pattern configuration
                base_name = filename
                base_name = os.path.splitext(base_name)[0]  # Remove extension first
                
                print(f"Processing: {filename} -> base_name before: {base_name}")
                
                # FIRST: Remove technical suffixes (like _A01_01_RapidSeq50_POP7xl_Z)
                if pattern_config['suffix_pattern']:
                    base_name = re.sub(pattern_config['suffix_pattern'], '', base_name)
                    print(f"  -> after suffix removal: {base_name}")
                
                # THEN: Apply pattern-based F/R extraction
                if pattern_config['type'] == 'virus_suffix':
                    # Pattern: -hanta_F or -hanta_R (virus name before F/R)
                    # Remove the final _F or _R, keep virus name
                    base_name = re.sub(r'_[Ff]$', '', base_name)
                    base_name = re.sub(r'_[Rr]$', '', base_name)
                    base_name = re.sub(r'-[Ff]$', '', base_name)
                    base_name = re.sub(r'-[Rr]$', '', base_name)
                    # Clean up trailing separators
                    base_name = re.sub(r'[_-]+$', '', base_name)
                elif pattern_config['type'] == 'custom':
                    # For custom patterns, intelligently remove F/R while preserving separators
                    fwd_pattern = pattern_config['forward_pattern']
                    rev_pattern = pattern_config['reverse_pattern']
                    
                    # Check if pattern contains separator (like -F, _F)
                    if fwd_pattern.startswith(('-', '_')) and rev_pattern.startswith(('-', '_')):
                        # Remove F/R but intelligently handle virus names
                        # -FHanta -> -Hanta (keep separator + virus)
                        # -F -> remove completely (no virus name)
                        sep = fwd_pattern[0]
                        fwd_letter = fwd_pattern[1:] if len(fwd_pattern) > 1 else 'F'
                        rev_letter = rev_pattern[1:] if len(rev_pattern) > 1 else 'R'
                        
                        # Match pattern: separator + letter + optional text
                        # If there's text after (virus name), keep separator + text
                        # If no text after, remove separator + letter completely
                        def smart_replace(text, letter, separator):
                            # Match: separator + letter + optional virus name (or end of string)
                            pattern = re.escape(separator) + re.escape(letter) + r'([a-zA-Z]*)'
                            def replacer(match):
                                virus_name = match.group(1)
                                if virus_name:
                                    # Has virus name: keep separator + virus
                                    return separator + virus_name
                                else:
                                    # No virus name: remove everything
                                    return ''
                            return re.sub(pattern, replacer, text, flags=re.IGNORECASE)
                        
                        base_name = smart_replace(base_name, fwd_letter, sep)
                        base_name = smart_replace(base_name, rev_letter, sep)
                        
                        # Clean up any remaining trailing separators
                        base_name = re.sub(r'[_-]+$', '', base_name)
                    else:
                        # Simple string replacement for patterns without separator
                        base_name = base_name.replace(fwd_pattern, '').replace(rev_pattern, '')
                elif pattern_config['type'] == 'fhanta':
                    # Remove F/R letter but keep virus name: -FHanta -> -Hanta
                    base_name = re.sub(r'([_-])[Ff]([a-zA-Z]+)', r'\1\2', base_name)
                    base_name = re.sub(r'([_-])[Rr]([a-zA-Z]+)', r'\1\2', base_name)
                    # Also handle plain -F or -R or _F or _R (without virus name)
                    base_name = re.sub(r'[_-][Ff](?![a-zA-Z])', '', base_name)
                    base_name = re.sub(r'[_-][Rr](?![a-zA-Z])', '', base_name)
                    # Clean up trailing separators
                    base_name = re.sub(r'[_-]+$', '', base_name)
                else:  # standard
                    # Remove _F, _R, -F, -R completely
                    base_name = re.sub(r'[_-][FfRr]([_-]|$)', r'\1', base_name)
                    base_name = re.sub(r'[_-]+$', '', base_name)
                
                print(f"  -> base_name after: {base_name}")
                
                ab1_groups[base_name].append(filepath)
                
                # Read sequence info for display
                try:
                    record = SeqIO.read(filepath, 'abi' if not filename.endswith('.scf') else 'scf')
                    
                    # Detect actual direction from AB1 file
                    detected_direction = detect_sequence_direction(record, filename, pattern_config)
                    filename_direction = get_filename_direction(filename, pattern_config)
                    
                    # Check for mismatch
                    direction_mismatch = False
                    mismatch_warning = ''
                    if detected_direction and filename_direction:
                        if detected_direction != filename_direction:
                            direction_mismatch = True
                            mismatch_warning = f"Warning: Filename suggests {filename_direction} but data shows {detected_direction}"
                    
                    sequences_info.append({
                        'filename': filename,
                        'group': base_name,
                        'length': len(record.seq),
                        'avg_quality': sum(record.letter_annotations.get('phred_quality', [0])) / 
                                     max(len(record.letter_annotations.get('phred_quality', [1])), 1),
                        'sequence': str(record.seq),
                        'quality': list(record.letter_annotations.get('phred_quality', [])),
                        'base_positions': list(record.annotations.get('abif_raw', {}).get('PLOC2', [])) if hasattr(record, 'annotations') and 'abif_raw' in record.annotations else [],
                        'detected_direction': detected_direction or 'Unknown',
                        'filename_direction': filename_direction or 'Unknown',
                        'direction_mismatch': direction_mismatch,
                        'mismatch_warning': mismatch_warning
                    })
                except:
                    sequences_info.append({
                        'filename': filename,
                        'group': base_name,
                        'length': 0,
                        'avg_quality': 0,
                        'sequence': '',
                        'quality': [],
                        'detected_direction': 'Unknown',
                        'filename_direction': get_filename_direction(filename, pattern_config) or 'Unknown',
                        'direction_mismatch': False,
                        'mismatch_warning': ''
                    })
        
        if not ab1_groups:
            return jsonify({'success': False, 'message': 'No valid AB1/ABI/SCF files found'}), 400
        
        # Analyze F/R pairs for potential swapping
        sequences_info = analyze_fr_pairs(sequences_info)
        
        # Save reference file if provided
        reference_path = None
        if reference_file and reference_file.filename:
            reference_filename = secure_filename(reference_file.filename)
            reference_path = os.path.join(session_folder, reference_filename)
            reference_file.save(reference_path)
        
        # Generate consensus for each group
        tracy_available = check_tracy_installed()
        consensus_results = []
        
        for group_name, group_files in ab1_groups.items():
            print(f"\n=== Processing group: {group_name} ({len(group_files)} files) ===")
            
            if tracy_available:
                result = run_tracy_assemble(group_files, reference_path, virus_type, session_folder)
                method = "Tracy"
            else:
                result = run_python_assemble(group_files, virus_type)
                method = "Python"
            
            if result:
                result['filename'] = f"{group_name}_consensus"
                result['group'] = group_name
                result['file_count'] = len(group_files)
                # Get source file IDs from sequences_info
                source_ids = [seq.get('id') for seq in sequences_info 
                             if seq.get('group') == group_name and seq.get('id')]
                result['source_file_ids'] = source_ids
                consensus_results.append(result)
        
        # Store session info
        session['ab1_session_id'] = session_id
        session['uploaded_sequences'] = sequences_info
        session['consensus_results'] = consensus_results
        
        total_files = sum(len(group_files) for group_files in ab1_groups.values())
        
        # Store in session for saving to database later
        session['sequences'] = sequences_info
        session['consensus_results'] = consensus_results
        
        # Create message with pattern detection info
        message = f'{total_files} file(s) uploaded, created {len(consensus_results)} consensus sequence(s)'
        if detection_result['detected']:
            message += f" (Auto-detected: {detection_result['pattern_name']})"
        
        return jsonify({
            'success': True,
            'message': message,
            'sequences': sequences_info,
            'consensus': consensus_results,
            'method': method,
            'groups': len(ab1_groups),
            'pattern_detection': detection_result
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/save-edit', methods=['POST'])
def save_sequence_edit():
    """Save manually edited sequence to server for persistent consensus"""
    try:
        data = request.get_json()
        filename = data.get('filename')
        sequence = data.get('sequence')
        
        if not filename or not sequence:
            return jsonify({'success': False, 'message': 'Missing filename or sequence'}), 400
            
        session_id = session.get('ab1_session_id')
        if not session_id:
            return jsonify({'success': False, 'message': 'Session expired'}), 400
            
        upload_folder = current_app.config['UPLOAD_FOLDER']
        session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
        
        if not os.path.exists(session_folder):
            return jsonify({'success': False, 'message': 'Session folder not found'}), 400
            
        # Save edited sequence as FASTA
        fasta_filename = f"{filename}.edited.fasta"
        fasta_path = os.path.join(session_folder, fasta_filename)
        
        with open(fasta_path, 'w') as f:
            f.write(f">{filename}_edited\n{sequence}\n")
            
        return jsonify({'success': True, 'message': f'Edits saved for {filename}'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/consensus', methods=['POST'])
def generate_consensus():
    """Re-generate consensus with different virus type"""
    try:
        data = request.get_json()
        virus_type = data.get('virus_type', 'Other')
        quality_map = data.get('quality_map', {})
        enable_trimming = data.get('enable_trimming', True)
        custom_start_pattern = data.get('trim_start_pattern', None)
        custom_end_pattern = data.get('trim_end_pattern', None)
        
        print(f"\n=== Consensus Generation Request ===")
        print(f"Virus Type: {virus_type}")
        print(f"Enable Trimming: {enable_trimming}")
        print(f"Quality Map Received: {len(quality_map)} entries")
        if custom_start_pattern or custom_end_pattern:
            print(f"Custom Patterns: {custom_start_pattern} ... {custom_end_pattern}")
        if quality_map:
            print("Sample quality entries:")
            for i, (filename, quality) in enumerate(list(quality_map.items())[:5]):
                print(f"  {filename}: {quality}")
        
        # Store trimming config in session
        session['trimming_config'] = {
            'enabled': enable_trimming,
            'custom_start': custom_start_pattern,
            'custom_end': custom_end_pattern
        }
        
        session_id = session.get('ab1_session_id')
        if not session_id:
            return jsonify({'success': False, 'message': 'No sequences available. Please upload files first.'}), 400
        
        upload_folder = current_app.config['UPLOAD_FOLDER']
        session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
        
        if not os.path.exists(session_folder):
            return jsonify({'success': False, 'message': 'Session expired. Please upload files again.'}), 400
        
        # Find AB1 files in session folder and group them using the same pattern config
        from collections import defaultdict
        import re
        
        # Get pattern config from session (should have been saved during upload)
        pattern_config = session.get('fr_pattern_config', {
            'type': 'standard',
            'forward_pattern': '-F',
            'reverse_pattern': '-R',
            'suffix_pattern': r'_[A-Z]\d+.*$'
        })
        
        ab1_groups = defaultdict(list)
        for filename in os.listdir(session_folder):
            if filename.endswith(('.ab1', '.abi', '.scf')):
                filepath = os.path.join(session_folder, filename)
                
                # Extract base name using SAME logic as upload
                base_name = filename
                base_name = os.path.splitext(base_name)[0]  # Remove extension first
                
                # FIRST: Remove technical suffixes (like _A01_01_RapidSeq50_POP7xl_Z)
                if pattern_config.get('suffix_pattern'):
                    base_name = re.sub(pattern_config['suffix_pattern'], '', base_name)
                
                # THEN: Apply pattern-based F/R extraction (same logic as upload)
                if pattern_config['type'] == 'virus_suffix':
                    base_name = re.sub(r'_[Ff]$', '', base_name)
                    base_name = re.sub(r'_[Rr]$', '', base_name)
                    base_name = re.sub(r'-[Ff]$', '', base_name)
                    base_name = re.sub(r'-[Rr]$', '', base_name)
                    base_name = re.sub(r'[_-]+$', '', base_name)
                elif pattern_config['type'] == 'custom':
                    fwd_pattern = pattern_config.get('forward_pattern', '-F')
                    rev_pattern = pattern_config.get('reverse_pattern', '-R')
                    
                    if fwd_pattern.startswith(('-', '_')) and rev_pattern.startswith(('-', '_')):
                        sep = fwd_pattern[0]
                        fwd_letter = fwd_pattern[1:] if len(fwd_pattern) > 1 else 'F'
                        rev_letter = rev_pattern[1:] if len(rev_pattern) > 1 else 'R'
                        
                        def smart_replace(text, letter, separator):
                            pattern = re.escape(separator) + re.escape(letter) + r'([a-zA-Z]*)'
                            def replacer(match):
                                virus_name = match.group(1)
                                if virus_name:
                                    return separator + virus_name
                                else:
                                    return ''
                            return re.sub(pattern, replacer, text, flags=re.IGNORECASE)
                        
                        base_name = smart_replace(base_name, fwd_letter, sep)
                        base_name = smart_replace(base_name, rev_letter, sep)
                        base_name = re.sub(r'[_-]+$', '', base_name)
                    else:
                        base_name = base_name.replace(fwd_pattern, '').replace(rev_pattern, '')
                elif pattern_config['type'] == 'fhanta':
                    # Remove F/R letter but keep virus name: -FHanta -> -Hanta
                    base_name = re.sub(r'([_-])[Ff]([a-zA-Z]+)', r'\1\2', base_name)
                    base_name = re.sub(r'([_-])[Rr]([a-zA-Z]+)', r'\1\2', base_name)
                    base_name = re.sub(r'[_-][Ff](?![a-zA-Z])', '', base_name)
                    base_name = re.sub(r'[_-][Rr](?![a-zA-Z])', '', base_name)
                    base_name = re.sub(r'[_-]+$', '', base_name)
                else:  # standard
                    base_name = re.sub(r'[_-][FfRr]([_-]|$)', r'\1', base_name)
                    base_name = re.sub(r'[_-]+$', '', base_name)
                
                ab1_groups[base_name].append(filepath)
        
        if not ab1_groups:
            return jsonify({'success': False, 'message': 'No AB1 files found in session'}), 400
        
        # Find reference file if any
        reference_path = None
        for filename in os.listdir(session_folder):
            if filename.endswith(('.fasta', '.fa', '.fas')):
                reference_path = os.path.join(session_folder, filename)
                break
        
        # Re-run assembly with new virus type for each group
        tracy_available = check_tracy_installed()
        consensus_results = []
        skipped_groups = []
        failed_groups = []  # Track groups that failed assembly
        
        # Get sequences_info from session for source file IDs
        sequences_info = session.get('sequences_info', [])
        
        for group_name, group_files in ab1_groups.items():
            print(f"\n=== Re-processing group: {group_name} ({len(group_files)} files) ===")
            
            # Check quality - Filter out 'Needs Work' files
            filtered_group_files = list(group_files)  # Start with all files
            
            if quality_map:
                file_qualities = []
                temp_good_files = []
                
                for filepath in group_files:
                    filename = os.path.basename(filepath)
                    quality = quality_map.get(filename, 'Unknown')
                    file_qualities.append(quality)
                    
                    if quality in ['Excellent', 'Good', 'Acceptable']:
                        temp_good_files.append(filepath)
                
                print(f"  All qualities for group: {file_qualities}")
                
                # Logic:
                # 1. If we have some 'Good'/'Excellent' files, ONLY use those (filter out Needs Work/Poor)
                # 2. If ALL files are 'Needs Work'/'Poor', then we skip the group entirely
                
                if temp_good_files:
                    # We have at least one good file
                    if len(temp_good_files) < len(group_files):
                        print(f"  ℹ Filtering: Using {len(temp_good_files)}/{len(group_files)} files (only Passed files)")
                        filtered_group_files = temp_good_files
                    else:
                        print(f"  ✓ Keeping all files (all Passed)")
                else:
                    # No good files found - all are Needs Work/Poor (or we have no files)
                    if group_files:
                        print(f"  ✗ SKIPPING: All {len(group_files)} files failed quality check (Needs Work or Poor)")
                        skipped_groups.append(group_name)
                        continue
            
            # Use the filtered list for assembly
            if tracy_available:
                result = run_tracy_assemble(filtered_group_files, reference_path, virus_type, session_folder)
                method = "Tracy"
            else:
                result = run_python_assemble(filtered_group_files, virus_type)
                method = "Python"
            
            if result:
                result['filename'] = f"{group_name}_consensus"
                result['group'] = group_name
                result['file_count'] = len(group_files)
                # Get source file IDs from sequences_info
                source_ids = [seq.get('id') for seq in sequences_info 
                             if seq.get('group') == group_name and seq.get('id')]
                result['source_file_ids'] = source_ids
                consensus_results.append(result)
            else:
                # Assembly failed for this group
                print(f"  ✗ FAILED: Assembly failed for {group_name}")
                failed_groups.append(group_name)
        
        if not consensus_results:
            error_parts = []
            if skipped_groups:
                error_parts.append(f'{len(skipped_groups)} skipped (poor quality)')
            if failed_groups:
                error_parts.append(f'{len(failed_groups)} failed (assembly error)')
            
            if error_parts:
                return jsonify({
                    'success': False, 
                    'message': f'All groups failed: {" + ".join(error_parts)}. No consensus generated.',
                    'skipped': skipped_groups,
                    'failed': failed_groups
                }), 400
            return jsonify({'success': False, 'message': 'Consensus generation failed'}), 500
        
        session['consensus_results'] = consensus_results
        
        # Build detailed message
        message = f'Generated {len(consensus_results)} consensus sequence(s) using {method}'
        warnings = []
        if skipped_groups:
            warnings.append(f'{len(skipped_groups)} skipped (poor quality)')
        if failed_groups:
            warnings.append(f'{len(failed_groups)} failed (assembly error)')
        if warnings:
            message += f' ({" + ".join(warnings)})'
        
        return jsonify({
            'success': True,
            'message': message,
            'results': consensus_results,
            'method': method,
            'skipped': len(skipped_groups),
            'failed': len(failed_groups),
            'skipped_groups': skipped_groups,
            'failed_groups': failed_groups
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


def check_tracy_installed():
    """Check if Tracy command-line tool is available"""
    try:
        result = subprocess.run(['tracy', '--version'], 
                              capture_output=True, 
                              timeout=5)
        return result.returncode == 0
    except:
        return False


def run_tracy_assemble(ab1_files, reference_path, virus_type, session_folder):
    """Run Tracy assemble command on multiple AB1 files"""
    try:
        output_base = os.path.join(session_folder, 'consensus')
        logfile = os.path.join(session_folder, 'tracy.log')
        errfile = os.path.join(session_folder, 'tracy.err')
        
        # Build Tracy command
        cmd = ['tracy', 'assemble', '-o', output_base]
        
        # Add reference if provided
        if reference_path:
            cmd.extend(['-r', reference_path])
        
        # Add AB1 files
        cmd.extend(ab1_files)
        
        # Run Tracy
        with open(logfile, 'w') as log, open(errfile, 'w') as err:
            result = subprocess.run(cmd, stdout=log, stderr=err, timeout=120)
        
        if result.returncode != 0:
            # Tracy failed, read error
            with open(errfile, 'r') as err:
                error_msg = err.read()
                print(f"Tracy error: {error_msg}")
            return None
        
        # Read Tracy output
        fasta_file = output_base + '.fasta'
        json_file = output_base + '.json'
        
        if os.path.exists(fasta_file):
            # Read consensus from FASTA
            consensus_record = SeqIO.read(fasta_file, 'fasta')
            consensus_seq = str(consensus_record.seq)
            
            # Apply virus-specific trimming
            # Get custom trimming patterns from session
            trimming_config = session.get('trimming_config', {})
            custom_start = trimming_config.get('custom_start')
            custom_end = trimming_config.get('custom_end')
            
            trimmed_seq = trim_by_virus_pattern(consensus_seq, virus_type, custom_start, custom_end)
            
            return {
                'filename': 'consensus',
                'original_length': len(consensus_seq),
                'trimmed_length': len(trimmed_seq),
                'consensus': trimmed_seq,
                'method': 'Tracy'
            }
        
        return None
        
    except Exception as e:
        print(f"Tracy assembly error: {str(e)}")
        return None


def run_python_assemble(ab1_files, virus_type):
    """Python-based consensus using proper pairwise alignment"""
    try:
        sequences = []
        
        # Read all AB1 files
        for filepath in ab1_files:
            try:
                filename = os.path.basename(filepath)
                edited_path = filepath + ".edited.fasta"
                
                # Check if there's a manually edited version (priority)
                if os.path.exists(edited_path):
                    print(f"  Using EDITED sequence for {filename}")
                    record = SeqIO.read(edited_path, 'fasta')
                    # For edited sequence, we don't have trace data/quality from original
                    # So we use dummy high quality
                    sequences.append({
                        'sequence': str(record.seq),
                        'quality': [40] * len(record.seq),
                        'filename': filename,
                        'is_edited': True
                    })
                else:
                    record = SeqIO.read(filepath, 'abi' if not filepath.endswith('.scf') else 'scf')
                    sequences.append({
                        'sequence': str(record.seq),
                        'quality': record.letter_annotations.get('phred_quality', []),
                        'filename': filename,
                        'is_edited': False
                    })
            except:
                continue
        
        if not sequences:
            return None
        
        # If only one sequence, just trim it
        if len(sequences) == 1:
            seq = sequences[0]['sequence']
            qual = sequences[0]['quality']
            trimmed_seq, _ = trim_sequence_by_quality(seq, qual, threshold=20)
            
            # Get custom trimming patterns from session
            trimming_config = session.get('trimming_config', {})
            custom_start = trimming_config.get('custom_start')
            custom_end = trimming_config.get('custom_end')
            
            final_seq = trim_by_virus_pattern(trimmed_seq, virus_type, custom_start, custom_end)
            
            # Clean any ambiguity codes
            final_seq = ''.join([base if base in 'ATGCatgc' else '' for base in final_seq])
            
            return {
                'filename': 'consensus',
                'original_length': len(seq),
                'trimmed_length': len(final_seq),
                'consensus': final_seq,
                'method': 'Python'
            }
        
        # Multiple sequences - use proper pairwise alignment
        print(f"Assembling {len(sequences)} sequences using Python pairwise alignment")
        print(f"DEBUG: virus_type parameter = '{virus_type}'")
        
        # Trim all sequences first
        trimmed_sequences = []
        for seq_data in sequences:
            seq = seq_data['sequence']
            qual = seq_data['quality']
            trimmed_seq, trimmed_qual = trim_sequence_by_quality(seq, qual, threshold=20)
            if trimmed_seq:
                trimmed_sequences.append({
                    'sequence': trimmed_seq,
                    'quality': trimmed_qual,
                    'filename': seq_data['filename']
                })
        
        if not trimmed_sequences:
            return None
        
        # Start with the longest sequence as reference
        reference = max(trimmed_sequences, key=lambda x: len(x['sequence']))
        consensus_seq = reference['sequence']
        consensus_qual = reference['quality']
        
        print(f"Reference: {reference['filename']} ({len(consensus_seq)} bp)")
        
        # Align and merge each additional sequence
        for seq_data in trimmed_sequences:
            if seq_data == reference:
                continue
            
            print(f"Aligning: {seq_data['filename']} ({len(seq_data['sequence'])} bp)")
            
            # Try both forward and reverse complement
            seq_fwd = seq_data['sequence']
            seq_rev = str(Seq(seq_data['sequence']).reverse_complement())
            qual_fwd = seq_data['quality']
            qual_rev = list(reversed(seq_data['quality']))
            
            # Align forward
            aligner = PairwiseAligner()
            aligner.mode = 'global'
            aligner.match_score = 2
            aligner.mismatch_score = -1
            aligner.open_gap_score = -2
            aligner.extend_gap_score = -0.5
            
            try:
                align_fwd = aligner.align(consensus_seq, seq_fwd)
                score_fwd = align_fwd.score if align_fwd else 0
            except (OverflowError, MemoryError) as e:
                print(f"  Warning: Forward alignment overflow for {seq_data['filename']}, using simple score")
                # Fallback: use simple match count
                score_fwd = sum(1 for a, b in zip(consensus_seq, seq_fwd) if a == b)
                align_fwd = None
            
            # Align reverse complement
            try:
                align_rev = aligner.align(consensus_seq, seq_rev)
                score_rev = align_rev.score if align_rev else 0
            except (OverflowError, MemoryError) as e:
                print(f"  Warning: Reverse alignment overflow for {seq_data['filename']}, using simple score")
                # Fallback: use simple match count
                score_rev = sum(1 for a, b in zip(consensus_seq, seq_rev) if a == b)
                align_rev = None
            
            # Use better alignment
            if score_rev > score_fwd:
                print(f"  Using reverse complement (score: {score_rev})")
                if align_rev:
                    best_align = next(iter(align_rev))
                else:
                    # Fallback to simple concatenation if alignment failed
                    print(f"  Warning: Using fallback merge for {seq_data['filename']}")
                    consensus_seq = consensus_seq + seq_rev
                    consensus_qual.extend(qual_rev)
                    continue
                seq_to_use = seq_rev
                qual_to_use = qual_rev
            else:
                print(f"  Using forward (score: {score_fwd})")
                if align_fwd:
                    best_align = next(iter(align_fwd))
                else:
                    # Fallback to simple concatenation if alignment failed
                    print(f"  Warning: Using fallback merge for {seq_data['filename']}")
                    consensus_seq = consensus_seq + seq_fwd
                    consensus_qual.extend(qual_fwd)
                    continue
                seq_to_use = seq_fwd
                qual_to_use = qual_fwd
            
            # Merge aligned sequences
            aligned_ref = str(best_align[0])
            aligned_seq = str(best_align[1])
            
            # Build consensus from alignment
            new_consensus = []
            new_qual = []
            ref_idx = 0
            seq_idx = 0
            
            for i in range(len(aligned_ref)):
                ref_base = aligned_ref[i].upper()
                seq_base = aligned_seq[i].upper()
                
                # Clean ambiguity codes - only allow ATGC or gaps
                if ref_base not in 'ATGC-':
                    ref_base = 'N'
                if seq_base not in 'ATGC-':
                    seq_base = 'N'
                
                ref_q = consensus_qual[ref_idx] if ref_idx < len(consensus_qual) and ref_base != '-' else 0
                seq_q = qual_to_use[seq_idx] if seq_idx < len(qual_to_use) and seq_base != '-' else 0
                
                # Choose base with higher quality, skipping N's
                if ref_base == '-' and seq_base != '-' and seq_base != 'N':
                    new_consensus.append(seq_base)
                    new_qual.append(seq_q)
                elif seq_base == '-' and ref_base != '-' and ref_base != 'N':
                    new_consensus.append(ref_base)
                    new_qual.append(ref_q)
                elif ref_base == 'N' and seq_base != 'N' and seq_base != '-':
                    new_consensus.append(seq_base)
                    new_qual.append(seq_q)
                elif seq_base == 'N' and ref_base != 'N' and ref_base != '-':
                    new_consensus.append(ref_base)
                    new_qual.append(ref_q)
                elif ref_base == seq_base and ref_base != 'N' and ref_base != '-':
                    new_consensus.append(ref_base)
                    new_qual.append(max(ref_q, seq_q))
                elif ref_base != '-' and seq_base != '-' and ref_base != 'N' and seq_base != 'N':
                    # Mismatch - use higher quality base
                    if seq_q > ref_q:
                        new_consensus.append(seq_base)
                        new_qual.append(seq_q)
                    else:
                        new_consensus.append(ref_base)
                        new_qual.append(ref_q)
                elif ref_base != '-' and ref_base != 'N':
                    new_consensus.append(ref_base)
                    new_qual.append(ref_q)
                elif seq_base != '-' and seq_base != 'N':
                    new_consensus.append(seq_base)
                    new_qual.append(seq_q)
                # else: skip if both are gaps or N
                
                if ref_base != '-':
                    ref_idx += 1
                if seq_base != '-':
                    seq_idx += 1
            
            consensus_seq = ''.join(new_consensus)
            consensus_qual = new_qual
            
            print(f"  New consensus length: {len(consensus_seq)} bp")
        
        # Apply virus-specific trimming
        # Get custom trimming patterns from session
        trimming_config = session.get('trimming_config', {})
        custom_start = trimming_config.get('custom_start')
        custom_end = trimming_config.get('custom_end')
        
        final_seq = trim_by_virus_pattern(consensus_seq, virus_type, custom_start, custom_end)
        
        # Clean any remaining ambiguity codes
        final_seq = ''.join([base if base in 'ATGCatgc' else '' for base in final_seq])
        
        print(f"Final consensus: {len(final_seq)} bp (after virus trimming)")
        
        return {
            'filename': 'consensus',
            'original_length': len(consensus_seq),
            'trimmed_length': len(final_seq),
            'consensus': final_seq,
            'method': 'Python'
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"Python assembly error: {str(e)}")
        return None


def trim_by_virus_pattern(sequence, virus_type, custom_start=None, custom_end=None):
    """Apply virus-specific pattern trimming or custom primers (supports IUPAC ambiguity codes)"""
    import re
    
    # IUPAC ambiguity code mapping
    iupac_codes = {
        'R': '[AG]', 'Y': '[CT]', 'M': '[AC]', 'K': '[GT]',
        'S': '[GC]', 'W': '[AT]', 'H': '[ACT]', 'B': '[CGT]',
        'V': '[ACG]', 'D': '[AGT]', 'N': '[ACGT]',
        'A': 'A', 'C': 'C', 'G': 'G', 'T': 'T'
    }
    
    def convert_to_regex(pattern):
        """Convert primer sequence with IUPAC codes to regex pattern"""
        if not pattern:
            return None
        regex = ''
        for base in pattern.upper():
            regex += iupac_codes.get(base, base)
        return regex
    
    # Check if trimming is disabled
    trimming_config = session.get('trimming_config', {'enabled': True})
    if not trimming_config.get('enabled', True):
        print(f"  Trimming disabled, returning original sequence")
        return sequence
    
    # Use custom patterns if provided
    if custom_start or custom_end:
        start_pattern = custom_start or ''
        end_pattern = custom_end or ''
        print(f"  Using custom trimming patterns: '{start_pattern}' ... '{end_pattern}'")
    else:
        # Use default patterns for virus type
        patterns = {
            "Hanta": ("TGGTCACC", "CATCATTC"),
            "Corona": ("AAGTGTGA", "ATGATTCT"),
            "Paramyxo": ("GGAATAAT", "ATGACCT"),
            "Flavi": ("AGAAGTTG", "CTCTCCAT"),
            "Other": None
        }
        
        if virus_type not in patterns or patterns[virus_type] is None:
            print(f"  No default patterns for {virus_type}, skipping trimming")
            return sequence
        
        start_pattern, end_pattern = patterns[virus_type]
    
    # Only trim if both patterns are provided
    if not start_pattern or not end_pattern:
        print(f"  Missing start or end pattern, skipping trimming")
        return sequence
    
    # Convert patterns to regex (handles IUPAC codes like N, R, Y, etc.)
    start_regex = convert_to_regex(start_pattern)
    end_regex = convert_to_regex(end_pattern)
    
    try:
        # Search for patterns using regex
        start_match = re.search(start_regex, sequence, re.IGNORECASE)
        end_match = None
        
        # Find last occurrence of end pattern
        for match in re.finditer(end_regex, sequence, re.IGNORECASE):
            end_match = match
        
        if start_match and end_match and start_match.start() < end_match.start():
            start_index = start_match.start()
            end_index = end_match.end()
            trimmed = sequence[start_index:end_index]
            print(f"  Trimmed by {virus_type} pattern: {len(sequence)} bp -> {len(trimmed)} bp")
            print(f"    Found start at position {start_index}, end at position {end_index}")
            if custom_start or custom_end:
                print(f"    Matched: '{sequence[start_index:start_match.end()]}' ... '{sequence[end_match.start():end_index]}'")
            return trimmed
        else:
            print(f"  Warning: Trimming patterns not found, returning original sequence")
            if not start_match:
                print(f"    Start pattern '{start_pattern}' not found")
                print(f"    Sequence starts with: {sequence[:50]}")
            if not end_match:
                print(f"    End pattern '{end_pattern}' not found")
                print(f"    Sequence ends with: {sequence[-50:]}")
            return sequence
    except Exception as e:
        print(f"  Error during pattern matching: {e}")
        return sequence


def trim_sequence_by_quality(sequence, quality, threshold=20):
    """Trim sequence ends based on quality threshold"""
    if not quality or len(quality) != len(sequence):
        return sequence, []
    
    # Find first position with quality >= threshold
    start = 0
    for i, q in enumerate(quality):
        if q >= threshold:
            start = i
            break
    
    # Find last position with quality >= threshold
    end = len(sequence)
    for i in range(len(quality) - 1, -1, -1):
        if quality[i] >= threshold:
            end = i + 1
            break
    
    return sequence[start:end], quality[start:end]



@sequence_bp.route('/export-fasta', methods=['POST'])
def export_fasta():
    """Export consensus sequences to FASTA format"""
    try:
        # Get consensus from session (try plural first, fallback to singular for backward compatibility)
        consensus_results = session.get('consensus_results')
        
        if not consensus_results:
            # Fallback to old single result format
            consensus_result = session.get('consensus_result')
            if consensus_result:
                consensus_results = [consensus_result]
        
        if not consensus_results:
            return jsonify({'success': False, 'message': 'No consensus to export'}), 400
        
        # Generate FASTA content for all consensus sequences
        fasta_content = ""
        for result in consensus_results:
            # Use group name (sample ID) instead of filename to match database records
            group = result.get('group', '')
            sequence = result.get('consensus', '')
            file_count = result.get('file_count', 1)
            
            if sequence:
                # Use group name as header to match BLAST and database naming
                sample_name = group if group else result.get('filename', 'consensus')
                header = f">{sample_name}"
                if file_count > 1:
                    header += f" files={file_count}"
                header += f" length={len(sequence)}"
                
                fasta_content += f"{header}\n{sequence}\n"
        
        if not fasta_content:
            return jsonify({'success': False, 'message': 'Empty consensus sequences'}), 400
        
        # Save to file
        timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        fasta_filename = f'consensus_{timestamp}.fasta'
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], fasta_filename)
        
        with open(filepath, 'w') as f:
            f.write(fasta_content)
        
        return jsonify({
            'success': True,
            'message': f'FASTA file generated with {len(consensus_results)} sequence(s)',
            'filename': fasta_filename,
            'download_url': f'/sequence/download/{fasta_filename}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/download/<filename>')
def download_fasta(filename):
    """Download FASTA file"""
    try:
        filepath = os.path.join(current_app.config['UPLOAD_FOLDER'], filename)
        
        if not os.path.exists(filepath):
            return jsonify({'success': False, 'message': 'File not found'}), 404
        
        return send_file(
            filepath,
            mimetype='text/plain',
            as_attachment=True,
            download_name=filename
        )
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/view-chromatogram/<filename>')
def view_chromatogram(filename):
    """View AB1 chromatogram for a specific file"""
    try:
        session_id = session.get('ab1_session_id')
        if not session_id:
            return jsonify({'success': False, 'message': 'No active session'}), 400
        
        # Get pattern config from session
        pattern_config = session.get('fr_pattern_config', {
            'type': 'standard',
            'forward_pattern': '-F',
            'reverse_pattern': '-R',
            'suffix_pattern': r'_[A-Z]\d+.*$'
        })
        
        upload_folder = current_app.config['UPLOAD_FOLDER']
        session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
        
        # Get the clicked file path
        filepath = os.path.join(session_folder, filename)
        if not os.path.exists(filepath):
            return jsonify({'success': False, 'message': 'File not found'}), 404
        
        # Detect if current file is Forward or Reverse
        current_direction = get_filename_direction(filename, pattern_config)
        
        # Extract base name for pairing (remove F/R indicators and extensions)
        # Use EXACT SAME logic as upload processing
        base_name = filename
        # Remove extension first
        name_parts = base_name.rsplit('.', 1)
        base_without_ext = name_parts[0]
        extension = '.' + name_parts[1] if len(name_parts) > 1 else ''
        
        # FIRST: Remove technical suffixes (like _A01_01_RapidSeq50_POP7xl_Z)
        suffix_pattern = pattern_config.get('suffix_pattern', r'_[A-Z]\d+.*$')
        if suffix_pattern:
            base_without_ext = re.sub(suffix_pattern, '', base_without_ext)
        
        # THEN: Apply pattern-based F/R extraction
        if pattern_config['type'] == 'virus_suffix':
            # Pattern: -hanta_F or -hanta_R (virus name before F/R)
            base_without_ext = re.sub(r'_[Ff]$', '', base_without_ext)
            base_without_ext = re.sub(r'_[Rr]$', '', base_without_ext)
            base_without_ext = re.sub(r'-[Ff]$', '', base_without_ext)
            base_without_ext = re.sub(r'-[Rr]$', '', base_without_ext)
            base_without_ext = re.sub(r'[_-]+$', '', base_without_ext)
        elif pattern_config['type'] == 'custom':
            fwd_pattern = pattern_config.get('forward_pattern', '-F')
            rev_pattern = pattern_config.get('reverse_pattern', '-R')
            
            # Check if pattern contains separator (like -F, _F)
            if fwd_pattern.startswith(('-', '_')) and rev_pattern.startswith(('-', '_')):
                # Remove F/R but intelligently handle virus names
                sep = fwd_pattern[0]
                fwd_letter = fwd_pattern[1:] if len(fwd_pattern) > 1 else 'F'
                rev_letter = rev_pattern[1:] if len(rev_pattern) > 1 else 'R'
                
                def smart_replace(text, letter, separator):
                    pattern = re.escape(separator) + re.escape(letter) + r'([a-zA-Z]*)'
                    def replacer(match):
                        virus_name = match.group(1)
                        if virus_name:
                            return separator + virus_name
                        else:
                            return ''
                    return re.sub(pattern, replacer, text, flags=re.IGNORECASE)
                
                base_without_ext = smart_replace(base_without_ext, fwd_letter, sep)
                base_without_ext = smart_replace(base_without_ext, rev_letter, sep)
                base_without_ext = re.sub(r'[_-]+$', '', base_without_ext)
            else:
                # Simple string replacement for patterns without separator
                base_without_ext = base_without_ext.replace(fwd_pattern, '').replace(rev_pattern, '')
        elif pattern_config['type'] == 'fhanta':
            # Remove F/R letter but keep virus name: -FHanta -> -Hanta
            base_without_ext = re.sub(r'([_-])[Ff]([a-zA-Z]+)', r'\1\2', base_without_ext)
            base_without_ext = re.sub(r'([_-])[Rr]([a-zA-Z]+)', r'\1\2', base_without_ext)
            # Also handle plain -F or -R or _F or _R (without virus name)
            base_without_ext = re.sub(r'[_-][Ff](?![a-zA-Z])', '', base_without_ext)
            base_without_ext = re.sub(r'[_-][Rr](?![a-zA-Z])', '', base_without_ext)
            # Clean up trailing separators
            base_without_ext = re.sub(r'[_-]+$', '', base_without_ext)
        else:  # standard
            # Remove _F, _R, -F, -R completely
            base_without_ext = re.sub(r'[_-][FfRr]([_-]|$)', r'\1', base_without_ext)
            base_without_ext = re.sub(r'[_-]+$', '', base_without_ext)
        
        print(f"\n=== Chromatogram View Debug ===")
        print(f"Clicked file: {filename}")
        print(f"Detected direction: {current_direction}")
        print(f"Base name without ext: {base_without_ext}")
        print(f"Pattern config type: {pattern_config['type']}")
        print(f"Suffix pattern: {suffix_pattern}")
        
        forward_file = None
        reverse_file = None
        
        # Look for forward and reverse files with matching base name
        for fname in os.listdir(session_folder):
            if not fname.endswith(('.ab1', '.abi', '.scf')):
                continue
            
            # Get base name of this file using SAME logic
            fname_parts = fname.rsplit('.', 1)
            fname_base = fname_parts[0]
            
            print(f"\nChecking file: {fname}")
            print(f"  Original base: {fname_base}")
            
            # FIRST: Remove technical suffixes
            if suffix_pattern:
                fname_base = re.sub(suffix_pattern, '', fname_base)
                print(f"  After suffix: {fname_base}")
            
            # THEN: Remove F/R indicators using same pattern logic
            if pattern_config['type'] == 'virus_suffix':
                fname_base = re.sub(r'_[Ff]$', '', fname_base)
                fname_base = re.sub(r'_[Rr]$', '', fname_base)
                fname_base = re.sub(r'-[Ff]$', '', fname_base)
                fname_base = re.sub(r'-[Rr]$', '', fname_base)
                fname_base = re.sub(r'[_-]+$', '', fname_base)
            elif pattern_config['type'] == 'custom':
                fwd_pattern = pattern_config.get('forward_pattern', '-F')
                rev_pattern = pattern_config.get('reverse_pattern', '-R')
                
                if fwd_pattern.startswith(('-', '_')) and rev_pattern.startswith(('-', '_')):
                    sep = fwd_pattern[0]
                    fwd_letter = fwd_pattern[1:] if len(fwd_pattern) > 1 else 'F'
                    rev_letter = rev_pattern[1:] if len(rev_pattern) > 1 else 'R'
                    
                    def smart_replace(text, letter, separator):
                        pattern = re.escape(separator) + re.escape(letter) + r'([a-zA-Z]*)'
                        def replacer(match):
                            virus_name = match.group(1)
                            if virus_name:
                                return separator + virus_name
                            else:
                                return ''
                        return re.sub(pattern, replacer, text, flags=re.IGNORECASE)
                    
                    fname_base = smart_replace(fname_base, fwd_letter, sep)
                    fname_base = smart_replace(fname_base, rev_letter, sep)
                    fname_base = re.sub(r'[_-]+$', '', fname_base)
                else:
                    fname_base = fname_base.replace(fwd_pattern, '').replace(rev_pattern, '')
            elif pattern_config['type'] == 'fhanta':
                fname_base = re.sub(r'([_-])[Ff]([a-zA-Z]+)', r'\1\2', fname_base)
                fname_base = re.sub(r'([_-])[Rr]([a-zA-Z]+)', r'\1\2', fname_base)
                fname_base = re.sub(r'[_-][Ff](?![a-zA-Z])', '', fname_base)
                fname_base = re.sub(r'[_-][Rr](?![a-zA-Z])', '', fname_base)
                fname_base = re.sub(r'[_-]+$', '', fname_base)
            else:  # standard
                fname_base = re.sub(r'[_-][FfRr]([_-]|$)', r'\1', fname_base)
                fname_base = re.sub(r'[_-]+$', '', fname_base)
            
            print(f"  Final base: {fname_base}")
            
            # Check if base names match
            if fname_base == base_without_ext:
                print(f"  *** MATCH FOUND! ***")
                fname_dir = get_filename_direction(fname, pattern_config)
                print(f"  Direction: {fname_dir}")
                if fname_dir == 'Forward':
                    forward_file = os.path.join(session_folder, fname)
                    print(f"  Set as forward: {fname}")
                elif fname_dir == 'Reverse':
                    reverse_file = os.path.join(session_folder, fname)
                    print(f"  Set as reverse: {fname}")
            else:
                print(f"  No match (target: {base_without_ext})")
        
        print(f"\nFinal result: forward_file={forward_file is not None}, reverse_file={reverse_file is not None}")
        print(f"===============================\n")
        
        # If current file wasn't categorized, use it as forward
        if current_direction == 'Forward' and not forward_file:
            forward_file = filepath
        elif current_direction == 'Reverse' and not reverse_file:
            reverse_file = filepath
        elif not forward_file and not reverse_file:
            forward_file = filepath
        
        def read_ab1_data(file_path):
            record = SeqIO.read(file_path, 'abi' if not file_path.endswith('.scf') else 'scf')
            
            trace_data = {'A': [], 'C': [], 'G': [], 'T': []}
            if hasattr(record, 'annotations') and 'abif_raw' in record.annotations:
                abif_raw = record.annotations['abif_raw']
                
                # Get the channel order from FWO_ tag (filter wheel order)
                # This tells us which DATA channel corresponds to which base
                fwo = abif_raw.get('FWO_', b'GATC').decode('ascii') if isinstance(abif_raw.get('FWO_', b'GATC'), bytes) else str(abif_raw.get('FWO_', 'GATC'))
                
                # Read the four data channels
                data_channels = [
                    list(abif_raw.get('DATA9', [])),
                    list(abif_raw.get('DATA10', [])),
                    list(abif_raw.get('DATA11', [])),
                    list(abif_raw.get('DATA12', []))
                ]
                
                # Map channels to bases according to FWO order
                for i, base in enumerate(fwo):
                    if i < len(data_channels) and base in 'ACGT':
                        trace_data[base] = data_channels[i]
            
            base_positions = []
            if hasattr(record, 'annotations') and 'abif_raw' in record.annotations:
                base_positions = list(record.annotations['abif_raw'].get('PLOC2', []))
            
            quality = list(record.letter_annotations.get('phred_quality', []))
            sequence = str(record.seq)
            
            return {
                'sequence': sequence,
                'quality': quality,
                'trace_data': trace_data,
                'base_positions': base_positions
            }
        
        # Detect direction from filename
        detected_direction = get_filename_direction(filename, pattern_config)
        if not detected_direction:
            # Try reading from AB1 metadata
            record = SeqIO.read(filepath, 'abi' if not filepath.endswith('.scf') else 'scf')
            detected_direction = detect_sequence_direction(record, filename, pattern_config)
        
        # Read forward data
        forward_data = read_ab1_data(forward_file if forward_file else filepath)
        forward_filename = os.path.basename(forward_file) if forward_file else filename
        
        # Read reverse data if available
        reverse_data = None
        reverse_filename = None
        if reverse_file and os.path.exists(reverse_file):
            reverse_data = read_ab1_data(reverse_file)
            reverse_filename = os.path.basename(reverse_file)
        
        return jsonify({
            'success': True,
            'filename': filename,
            'forward_filename': forward_filename,
            'reverse_filename': reverse_filename,
            'detected_direction': detected_direction,
            'has_reverse': reverse_data is not None,
            'forward': {
                'sequence': forward_data['sequence'],
                'length': len(forward_data['sequence']),
                'quality': forward_data['quality'],
                'avg_quality': sum(forward_data['quality']) / len(forward_data['quality']) if forward_data['quality'] else 0,
                'trace_data': forward_data['trace_data'],
                'base_positions': forward_data['base_positions']
            },
            'reverse': reverse_data if reverse_data else None
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


def generate_chromatogram_image(trace_data, sequence, quality, start=0, end=None, window=100):
    """Generate chromatogram image as base64"""
    try:
        if not end:
            end = min(start + window, len(sequence))
        
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(15, 6), 
                                        gridspec_kw={'height_ratios': [3, 1]})
        fig.patch.set_facecolor('#1a1a1a')
        
        # Plot traces
        colors = {'A': 'green', 'C': 'blue', 'G': 'black', 'T': 'red'}
        
        for base, color in colors.items():
            if base in trace_data and trace_data[base]:
                trace = trace_data[base][start*4:(end+1)*4]
                x = np.arange(len(trace))
                ax1.plot(x, trace, color=color, linewidth=1, label=base)
        
        ax1.set_xlim(0, (end - start) * 4)
        ax1.set_ylabel('Signal Intensity', color='white')
        ax1.set_facecolor('#2a2a2a')
        ax1.tick_params(colors='white')
        ax1.spines['bottom'].set_color('white')
        ax1.spines['top'].set_color('white')
        ax1.spines['left'].set_color('white')
        ax1.spines['right'].set_color('white')
        ax1.legend(loc='upper right', facecolor='#2a2a2a', edgecolor='white', labelcolor='white')
        ax1.set_title(f'Chromatogram (Position {start}-{end})', color='white')
        
        # Plot base calls
        seq_window = sequence[start:end]
        qual_window = quality[start:end] if quality else []
        
        x_pos = np.arange(len(seq_window))
        base_colors = [colors.get(base, 'gray') for base in seq_window]
        
        for i, (base, color) in enumerate(zip(seq_window, base_colors)):
            ax1.text((i * 4) + 2, ax1.get_ylim()[1] * 0.9, base, 
                    color=color, fontsize=8, ha='center', weight='bold')
        
        # Plot quality scores
        if qual_window:
            ax2.bar(x_pos, qual_window, color='cyan', alpha=0.7)
            ax2.axhline(y=20, color='red', linestyle='--', linewidth=1, label='Q20 threshold')
            ax2.set_xlabel('Base Position', color='white')
            ax2.set_ylabel('Quality Score', color='white')
            ax2.set_facecolor('#2a2a2a')
            ax2.tick_params(colors='white')
            ax2.spines['bottom'].set_color('white')
            ax2.spines['top'].set_color('white')
            ax2.spines['left'].set_color('white')
            ax2.spines['right'].set_color('white')
            ax2.legend(loc='upper right', facecolor='#2a2a2a', edgecolor='white', labelcolor='white')
            ax2.set_xlim(-0.5, len(seq_window) - 0.5)
        
        plt.tight_layout()
        
        # Convert to base64
        buffer = BytesIO()
        plt.savefig(buffer, format='png', facecolor='#1a1a1a', dpi=100)
        buffer.seek(0)
        img_base64 = base64.b64encode(buffer.read()).decode()
        plt.close()
        
        return img_base64
        
    except Exception as e:
        print(f"Error generating chromatogram: {str(e)}")
        return None


@sequence_bp.route('/save-ab1', methods=['POST'])
def save_ab1():
    """Save corrected sequence as FASTA file (AB1 writing not supported by Biopython)"""
    try:
        data = request.json
        consensus_sequence = data.get('consensus_sequence', '')
        original_filename = data.get('original_file', 'sequence.ab1')
        
        # Get the original AB1 file from session uploads
        session_id = session.get('session_id')
        if not session_id:
            return jsonify({'success': False, 'message': 'No session found'}), 400
        
        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
        
        # Create FASTA output
        base_name = original_filename.replace('.ab1', '').replace('.abi', '')
        output_filename = f"{base_name}_corrected.fasta"
        output_path = os.path.join(session_folder, output_filename)
        
        # Write FASTA file
        with open(output_path, 'w') as f:
            f.write(f">{base_name}_corrected\n")
            # Write sequence in lines of 80 characters
            for i in range(0, len(consensus_sequence), 80):
                f.write(consensus_sequence[i:i+80] + '\n')
        
        # Send file to user
        return send_file(output_path, as_attachment=True, download_name=output_filename, mimetype='text/plain')
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'message': str(e)}), 500


def save_blast_results_to_db(conn, db_type, blast_results, consensus_results):
    """Save BLAST results to database using blast_results and blast_hits tables"""
    import json
    from datetime import datetime
    
    cursor = conn.cursor()
    
    # Create a mapping of sequence names to consensus IDs
    name_to_id = {}
    cursor.execute("SELECT id, consensus_name FROM consensus_sequences")
    for row in cursor.fetchall():
        name_to_id[row[1]] = row[0]
    
    for blast_result in blast_results:
        seq_name = blast_result.get('name')
        if not seq_name or seq_name not in name_to_id:
            continue
        
        consensus_id = name_to_id[seq_name]
        seq_length = blast_result.get('sequence_length', 0)
        
        # Check if there was an error
        if 'error' in blast_result:
            # Insert failed BLAST result
            if db_type == 'mysql':
                cursor.execute("""
                    INSERT INTO blast_results 
                    (consensus_id, blast_date, query_name, query_length, total_hits, status, error_message)
                    VALUES (%s, %s, %s, %s, 0, 'failed', %s)
                """, (consensus_id, datetime.now(), seq_name, seq_length, blast_result['error']))
            else:
                cursor.execute("""
                    INSERT INTO blast_results 
                    (consensus_id, blast_date, query_name, query_length, total_hits, status, error_message)
                    VALUES (?, ?, ?, ?, 0, 'failed', ?)
                """, (consensus_id, datetime.now(), seq_name, seq_length, blast_result['error']))
            continue
        
        hits = blast_result.get('hits', [])
        total_hits = len(hits)
        
        # Insert BLAST result
        status = 'no_hits' if total_hits == 0 else 'success'
        
        if db_type == 'mysql':
            cursor.execute("""
                INSERT INTO blast_results 
                (consensus_id, blast_date, query_name, query_length, total_hits, status)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (consensus_id, datetime.now(), seq_name, seq_length, total_hits, status))
            blast_result_id = cursor.lastrowid
        else:
            cursor.execute("""
                INSERT INTO blast_results 
                (consensus_id, blast_date, query_name, query_length, total_hits, status)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (consensus_id, datetime.now(), seq_name, seq_length, total_hits, status))
            blast_result_id = cursor.lastrowid
        
        # Insert individual hits
        for rank, hit in enumerate(hits, 1):
            if db_type == 'mysql':
                cursor.execute("""
                    INSERT INTO blast_hits 
                    (blast_result_id, hit_rank, accession, title, query_coverage, 
                     identity_percent, evalue, bit_score, align_length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    blast_result_id,
                    rank,
                    hit.get('accession', ''),
                    hit.get('title', '')[:500],  # Limit title length
                    hit.get('query_coverage'),
                    hit.get('identity_percent'),
                    hit.get('evalue'),
                    hit.get('bit_score'),
                    hit.get('align_len')
                ))
            else:
                cursor.execute("""
                    INSERT INTO blast_hits 
                    (blast_result_id, hit_rank, accession, title, query_coverage, 
                     identity_percent, evalue, bit_score, align_length)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    blast_result_id,
                    rank,
                    hit.get('accession', ''),
                    hit.get('title', '')[:500],
                    hit.get('query_coverage'),
                    hit.get('identity_percent'),
                    hit.get('evalue'),
                    hit.get('bit_score'),
                    hit.get('align_len')
                ))
    
    conn.commit()
    print(f"✓ Saved {len(blast_results)} BLAST results with hits to database")


@sequence_bp.route('/blast-consensus', methods=['POST'])
def blast_consensus():
    """Run BLAST on all consensus sequences using BioPython NCBIWWW"""
    try:
        data = request.get_json()
        consensus_results = session.get('consensus_results', [])
        mode = data.get('mode', 'viruses')  # 'viruses' or 'all'
        program_override = data.get('program', 'auto') # 'auto', 'blastn', 'megablast'
        
        if not consensus_results:
            return jsonify({'success': False, 'message': 'No consensus sequences available'}), 400
        
        total = len(consensus_results)
        mode_text = 'viruses only' if mode == 'viruses' else 'all organisms'
        print(f"\n=== Starting BLAST for {total} sequences (Mode: {mode_text}, Program: {program_override}) ===")
        
        blast_results = []
        completed = 0
        
        # Initialize global progress
        global blast_progress_data
        with progress_lock:
            blast_progress_data = {'completed': 0, 'total': total, 'status': 'running'}
        
        def blast_batch(batch_results):
            """Run BLAST for a batch of sequences using BioPython with retry logic"""
            # Create a multi-FASTA string for the batch
            fasta_string = ""
            batch_names = []
            for res in batch_results:
                name = res.get('group', res.get('filename', 'Unknown'))
                seq = res.get('consensus', '')
                if seq:
                    fasta_string += f">{name}\n{seq}\n"
                    batch_names.append(name)
            
            # Check if cancelled before starting
            with progress_lock:
                if blast_progress_data.get('cancelled', False):
                    print(f"  ⚠ BLAST batch cancelled before starting for: {', '.join(batch_names[:2])}")
                    return [{'name': name, 'error': 'BLAST cancelled by user'} for name in batch_names]
            
            if not fasta_string:
                return [{'name': 'Unknown', 'error': 'No sequence data in batch'}]
            
            # Prepare BLAST parameters
            blast_params = {
                'program': "blastn",
                'database': "nt",
                'sequence': fasta_string,
                'hitlist_size': 10,
                'expect': 10,
                'format_type': "XML"
            }
            
            # Configure algorithm (using common settings for the batch)
            # Defaulting to most efficient for mix if auto
            if program_override == 'blastn':
                blast_params['service'] = 'plain' 
            elif program_override == 'megablast':
                blast_params['megablast'] = True
            elif program_override == 'blastx':
                blast_params['program'] = 'blastx'
                blast_params['database'] = 'nr'
            else: # Auto - use megablast for efficiency in batches
                blast_params['megablast'] = True
            
            if mode == 'viruses':
                blast_params['entrez_query'] = "viruses[organism]"

            # Retry logic settings
            max_retries = 3
            retry_delay = 5
            
            for attempt in range(max_retries + 1):
                try:
                    if attempt > 0:
                        print(f"  Attempt {attempt + 1}/{max_retries + 1} for batch of {len(batch_names)} after {retry_delay}s delay...")
                        time.sleep(retry_delay)
                        retry_delay *= 2
                    
                    print(f"BLASTing Batch: {', '.join(batch_names[:2])}{'...' if len(batch_names) > 2 else ''} ({len(batch_names)} sequences)")
                    
                    # Run BLAST
                    result_handle = NCBIWWW.qblast(**blast_params)
                    
                    # Parse XML results - Multi-sequence results have multiple iterations
                    blast_records = NCBIXML.parse(result_handle)
                    
                    results = []
                    for i, blast_record in enumerate(blast_records):
                        if i >= len(batch_names): break # Should not happen
                        
                        seq_name = batch_names[i]
                        orig_seq = next((r.get('consensus', '') for r in batch_results if r.get('group', r.get('filename')) == seq_name), '')
                        seq_len = len(orig_seq)
                        
                        hits = []
                        for rank, alignment in enumerate(blast_record.alignments[:10], 1):
                            hsp = alignment.hsps[0]
                            identity_percent = (hsp.identities / hsp.align_length) * 100 if hsp.align_length > 0 else 0
                            query_coverage = (hsp.align_length / seq_len) * 100 if seq_len > 0 else 0
                            
                            # Extract organism from title if available
                            title = alignment.title
                            organism = ''
                            if '[' in title and ']' in title:
                                organism = title[title.find('[')+1:title.find(']')]
                            
                            hits.append({
                                'hit_rank': rank,
                                'title': title,
                                'accession': alignment.accession,
                                'organism': organism,
                                'identity': hsp.identities,
                                'align_length': hsp.align_length,
                                'evalue': hsp.expect,
                                'bit_score': hsp.bits,
                                'query_coverage': round(query_coverage, 2),
                                'identity_percent': round(identity_percent, 2),
                                'query_from': hsp.query_start,
                                'query_to': hsp.query_end,
                                'hit_from': hsp.sbjct_start,
                                'hit_to': hsp.sbjct_end,
                                'gaps': getattr(hsp, 'gaps', 0)
                            })
                        
                        results.append({
                            'name': seq_name,
                            'sequence_length': seq_len,
                            'hits': hits
                        })
                        
                        if len(hits) == 0:
                            print(f"  ⚠ No hits found for {seq_name}")
                        else:
                            print(f"  ✓ {seq_name}: {len(hits)} hits")
                    
                    result_handle.close()
                    return results
                    
                except Exception as e:
                    error_msg = str(e)
                    print(f"Error BLASTing batch (Attempt {attempt + 1}): {error_msg}")
                    
                    if attempt == max_retries:
                        # Return errors for all sequences in the batch
                        return [{'name': name, 'error': error_msg} for name in batch_names]
                    continue
        
        # Split consensus results into batches of 5
        batch_size = 5
        batches = [consensus_results[i:i + batch_size] for i in range(0, len(consensus_results), batch_size)]
        
        # Run batches in parallel (using 2 workers to stay safe but efficient)
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(blast_batch, batch): batch for batch in batches}
            
            for future in as_completed(futures):
                try:
                    start_time = time.time()
                    batch_res_list = future.result()
                    elapsed = time.time() - start_time
                    
                    for res in batch_res_list:
                        blast_results.append(res)
                        completed += 1
                        
                        # Update global progress
                        with progress_lock:
                            blast_progress_data['completed'] = completed
                            blast_progress_data['status'] = 'running'
                    
                    print(f"Progress: {completed}/{total} completed - Batch took {elapsed:.1f}s")
                except Exception as e:
                    print(f"Unexpected error in BLAST batch thread: {e}")
                    # Try to recovered metadata from futures if possible
                    completed += batch_size # Approximate
        
        # Mark as completed
        with progress_lock:
            blast_progress_data['completed'] = total
            blast_progress_data['status'] = 'completed'
        
        print(f"\n=== Completed {len(blast_results)} BLAST searches ===")
        
        # Store results in session
        session['blast_results'] = blast_results
        
        # Clean up AB1 files after BLAST completion
        try:
            session_id = session.get('ab1_session_id')
            if session_id:
                upload_folder = current_app.config['UPLOAD_FOLDER']
                session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
                
                if os.path.exists(session_folder):
                    import shutil
                    removed = 0
                    for filename in os.listdir(session_folder):
                        if filename.lower().endswith(('.ab1', '.abi', '.scf')):
                            try:
                                os.remove(os.path.join(session_folder, filename))
                                removed += 1
                            except: pass
                    
                    if not os.listdir(session_folder):
                        shutil.rmtree(session_folder)
                    print(f"[CLEANUP] Removed {removed} files from session {session_id}")
        except Exception as e:
            print(f"[CLEANUP] Error: {e}")
        
        # Save to database if requested
        save_to_db = data.get('save_to_database', False)
        print(f"[DEBUG] save_to_database flag: {save_to_db}")
        if save_to_db:
            try:
                db_path = session.get('db_path')
                db_type = session.get('db_type', 'sqlite')
                if db_path:
                    conn = DatabaseManagerFlask.get_connection(db_path, db_type)
                    save_blast_results_to_db(conn, db_type, blast_results, consensus_results)
                    print("✓ BLAST results saved to database")
            except Exception as e:
                print(f"Warning: Could not save to database: {e}")
        else:
            print("[DEBUG] Skipping database save (save_to_database=False)")
        
        return jsonify({
            'success': True,
            'message': f'BLAST completed for {len(blast_results)} sequences',
            'results': blast_results
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        with progress_lock:
            blast_progress_data['status'] = 'error'
            blast_progress_data['error'] = str(e)
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/blast-cancel', methods=['POST'])
def cancel_blast():
    """Cancel running BLAST operation"""
    try:
        global blast_progress_data
        with progress_lock:
            if blast_progress_data['status'] == 'running':
                blast_progress_data['cancelled'] = True
                blast_progress_data['status'] = 'cancelled'
                return jsonify({'success': True, 'message': 'BLAST operation cancelled'})
            else:
                return jsonify({'success': False, 'message': 'No BLAST operation running'})
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@sequence_bp.route('/blast-progress', methods=['GET'])
def blast_progress():
    """Get BLAST progress status"""
    try:
        with progress_lock:
            progress = blast_progress_data.copy()
        return jsonify(progress)
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@sequence_bp.route('/save-to-database', methods=['POST'])
def save_to_database():
    """Save sequences, consensus, and BLAST results to database with real-time updates"""
    try:
        data = request.get_json()
        
        # Check if database is connected (check session first, fallback to request data)
        db_connected = session.get('db_connected', data.get('db_connected', False))
        db_type = session.get('db_type', data.get('db_type'))
        
        print(f"[DEBUG] Save to database request - Connected: {db_connected}, Type: {db_type}")
        print(f"[DEBUG] Session keys: {list(session.keys())}")
        print(f"[DEBUG] Request data keys: {list(data.keys())}")
        
        if not db_connected:
            return jsonify({
                'success': False,
                'message': 'No database connection configured. Please connect to a database first from the Database Connection page.'
            }), 400
        
        # Get database connection info from session or request data
        if db_type == 'sqlite':
            # Try session first, then request data
            db_conn = session.get('db_path') or data.get('db_path')
            
            # If still not found, try to reconstruct from db_name in request
            if not db_conn and data.get('db_name'):
                # Assuming the database is in the current directory or uploads folder
                db_name = data.get('db_name')
                possible_paths = [
                    db_name,  # Direct path
                    os.path.join(os.path.dirname(__file__), '..', db_name),  # Project root
                    os.path.join(os.path.dirname(__file__), '..', 'uploads', db_name),  # Uploads folder
                ]
                for path in possible_paths:
                    if os.path.exists(path):
                        db_conn = os.path.abspath(path)
                        break
            
            print(f"[DEBUG] SQLite path: {db_conn}")
            if not db_conn:
                return jsonify({
                    'success': False,
                    'message': 'SQLite database path not found. Please reconnect to the database.'
                }), 400
        else:  # mysql
            db_conn = session.get('db_params')
            print(f"[DEBUG] MySQL params: {db_conn}")
            if not db_conn:
                return jsonify({
                    'success': False,
                    'message': 'MySQL database parameters not found in session. Please reconnect to the database.'
                }), 400
        
        # Get SocketIO instance for real-time updates
        socketio = current_app.socketio if hasattr(current_app, 'socketio') else None
        
        # Emit start of save operation
        if socketio:
            socketio.emit('save_operation_started', {
                'operation': 'database_save',
                'message': 'Starting to save data to database...',
                'timestamp': datetime.datetime.now().isoformat()
            })
        
        # Initialize database manager
        print(f"[DEBUG] Initializing SequenceDBManager with type: {db_type}")
        seq_db = SequenceDBManager(db_conn, db_type)
        
        project_name = data.get('project_name', 'Default')
        uploaded_by = session.get('username', 'Anonymous')
        
        saved_count = 0
        consensus_ids = {}
        
        # Save project record
        try:
            cursor = seq_db._get_connection().cursor()
            if db_type == 'sqlite':
                cursor.execute("""
                    INSERT INTO projects (project_name, description, created_by, created_date)
                    VALUES (?, ?, ?, datetime('now'))
                """, (project_name, f"Sequences uploaded on {datetime.datetime.now().strftime('%Y-%m-%d')}", uploaded_by))
            else:
                cursor.execute("""
                    INSERT INTO projects (project_name, description, created_by, created_date)
                    VALUES (%s, %s, %s, NOW())
                """, (project_name, f"Sequences uploaded on {datetime.datetime.now().strftime('%Y-%m-%d')}", uploaded_by))
            seq_db._get_connection().commit()
            print(f"Created project: {project_name}")
            
            # Emit real-time update for project creation
            if socketio:
                socketio.emit('data_inserted', {
                    'table': 'projects',
                    'count': 1,
                    'action': 'insert',
                    'timestamp': datetime.datetime.now().isoformat()
                })
        except Exception as e:
            # Project might already exist, that's OK
            print(f"Project creation note: {e}")
        
        # Save uploaded sequences
        duplicate_sequences = 0
        if data.get('save_sequences'):
            sequences = session.get('sequences', [])
            total_sequences = len(sequences)
            
            for i, seq in enumerate(sequences):
                try:
                    # Parse filename to extract sample_id and target_sequence
                    filename = seq.get('filename', '')
                    sample_id, target_sequence = parse_sample_id_and_target(filename)
                    
                    seq_data = {
                        'filename': filename,
                        'sequence': seq.get('sequence'),
                        'sequence_length': seq.get('length'),
                        'group': seq.get('group'),
                        'detected_direction': seq.get('detected_direction'),
                        'quality_score': seq.get('quality_score'),
                        'avg_quality': seq.get('avg_quality'),
                        'min_quality': seq.get('min_quality'),
                        'max_quality': seq.get('max_quality'),
                        'overall_grade': seq.get('advancedAnalysis', {}).get('overallGrade'),
                        'grade_score': seq.get('advancedAnalysis', {}).get('gradeScore'),
                        'issues': seq.get('advancedAnalysis', {}).get('issues', []),
                        'likely_swapped': seq.get('likely_swapped', False),
                        'direction_mismatch': seq.get('direction_mismatch', False),
                        'complementarity_score': seq.get('complementarity_score'),
                        'ambiguity_count': seq.get('ambiguity_count'),
                        'ambiguity_percent': seq.get('ambiguity_percent'),
                        'virus_type': data.get('virus_type'),
                        'sample_id': sample_id,
                        'target_sequence': target_sequence,
                        'uploaded_by': uploaded_by,
                        'project_name': project_name
                    }
                    
                    result_id = seq_db.save_sequence(seq_data)
                    if result_id is not None:
                        saved_count += 1
                        print(f"[SAVED] New sequence: {filename}")
                        
                        # Emit progress update
                        if socketio and i % 5 == 0:  # Emit every 5 sequences to avoid spam
                            socketio.emit('save_progress', {
                                'current': i + 1,
                                'total': total_sequences,
                                'type': 'sequences',
                                'message': f"Saved {i + 1}/{total_sequences} sequences"
                            })
                    else:
                        duplicate_sequences += 1
                        print(f"[DUPLICATE] Skipped duplicate sequence: {filename}")
                except Exception as e:
                    print(f"Failed to save sequence {seq.get('filename')}: {e}")
            
            # Emit final sequences update
            if socketio and saved_count > 0:
                socketio.emit('data_inserted', {
                    'table': 'sequences',
                    'count': saved_count,
                    'action': 'insert',
                    'timestamp': datetime.datetime.now().isoformat()
                })
        
        # Save consensus sequences
        if data.get('save_consensus'):
            consensus_results = session.get('consensus_results', [])
            duplicate_consensus = 0
            total_consensus = len(consensus_results)
            
            for i, cons in enumerate(consensus_results):
                try:
                    # Parse sample_id and target_sequence from group name
                    group_name = cons.get('group') or cons.get('filename')
                    sample_id, target_sequence = parse_sample_id_and_target(group_name)
                    
                    consensus_data = {
                        'name': group_name,
                        'consensus': cons.get('consensus'),
                        'original_length': cons.get('original_length'),
                        'trimmed_length': cons.get('trimmed_length'),
                        'group': cons.get('group'),
                        'file_count': cons.get('file_count', 1),
                        'source_file_ids': cons.get('source_file_ids', []),
                        'sample_id': sample_id,
                        'target_sequence': target_sequence,
                        'virus_type': data.get('virus_type'),
                        'uploaded_by': uploaded_by,
                        'project_name': project_name
                    }
                    result_id = seq_db.save_consensus(consensus_data)
                    if result_id is not None:
                        consensus_ids[cons.get('group')] = result_id
                        saved_count += 1
                        print(f"[SAVED] New consensus: {group_name}")
                        
                        # Emit progress update
                        if socketio and i % 2 == 0:  # Emit every 2 consensus sequences
                            socketio.emit('save_progress', {
                                'current': i + 1,
                                'total': total_consensus,
                                'type': 'consensus',
                                'message': f"Saved {i + 1}/{total_consensus} consensus sequences"
                            })
                    else:
                        duplicate_consensus += 1
                        print(f"[DUPLICATE] Skipped duplicate consensus: {group_name}")
                except Exception as e:
                    print(f"Failed to save consensus {cons.get('group')}: {e}")
            
            # Emit final consensus update
            if socketio and len(consensus_ids) > 0:
                socketio.emit('data_inserted', {
                    'table': 'consensus_sequences',
                    'count': len(consensus_ids),
                    'action': 'insert',
                    'timestamp': datetime.datetime.now().isoformat()
                })
        
        # Save BLAST results
        if data.get('save_blast'):
            blast_results = session.get('blast_results', [])
            total_blast = len(blast_results)
            
            for i, blast in enumerate(blast_results):
                try:
                    group_name = blast.get('name')
                    consensus_id = consensus_ids.get(group_name)
                    
                    if not consensus_id:
                        print(f"No consensus ID found for {group_name}, skipping BLAST save")
                        continue
                    
                    blast_data = {
                        'query_name': blast.get('name'),
                        'query_length': blast.get('sequence_length'),
                        'hits': blast.get('hits', []),
                        'blast_mode': data.get('blast_mode', 'viruses'),
                        'error_message': blast.get('error'),
                        'database_used': 'nt',
                        'program': 'blastn',
                        'total_hits': len(blast.get('hits', [])),
                        'status': 'failed' if blast.get('error') else ('success' if blast.get('hits') else 'no_hits')
                    }
                    seq_db.save_blast_results(consensus_id, blast_data)
                    saved_count += 1
                    print(f"[SAVED] BLAST results for {group_name}")
                    
                    # Emit progress update
                    if socketio and i % 2 == 0:  # Emit every 2 BLAST results
                        socketio.emit('save_progress', {
                            'current': i + 1,
                            'total': total_blast,
                            'type': 'blast',
                            'message': f"Saved {i + 1}/{total_blast} BLAST results"
                        })
                except Exception as e:
                    print(f"Failed to save BLAST results for {blast.get('name')}: {e}")
            
            # Emit final BLAST update
            if socketio and total_blast > 0:
                socketio.emit('data_inserted', {
                    'table': 'blast_results',
                    'count': total_blast,
                    'action': 'insert',
                    'timestamp': datetime.datetime.now().isoformat()
                })
        
        # Schedule cleanup in background to avoid blocking
        if socketio:
            socketio.emit('save_progress', {
                'current': 0,
                'total': 1,
                'type': 'cleanup',
                'message': 'Cleaning up temporary files...'
            })
        
        # Start background cleanup with timeout
        import threading
        import time
        
        # Capture config value AND session ID HERE in the main thread context
        upload_folder_path = current_app.config['UPLOAD_FOLDER']
        session_id_val = session.get('ab1_session_id')
        
        def cleanup_with_timeout():
            # Use captured variable instead of accessing current_app or session
            local_upload_folder = upload_folder_path
            local_session_id = session_id_val
            app_socketio = socketio
            
            cleanup_thread = threading.Thread(target=background_cleanup, args=(local_session_id, local_upload_folder, app_socketio))
            cleanup_thread.daemon = True
            cleanup_thread.start()
            
            # Wait for cleanup to complete with timeout (30 seconds)
            cleanup_thread.join(timeout=30.0)
            
            if cleanup_thread.is_alive():
                print("[WARNING] Cleanup thread timed out after 30 seconds")
                if app_socketio:
                    app_socketio.emit('cleanup_error', {
                        'error': 'Cleanup operation timed out. Some temporary files may remain.',
                        'timestamp': datetime.datetime.now().isoformat()
                    })
        
        # Start cleanup in a separate thread to avoid blocking
        timeout_thread = threading.Thread(target=cleanup_with_timeout)
        timeout_thread.daemon = True
        timeout_thread.start()
        
        # Build detailed message with duplicate information
        message_parts = []
        total_items = 0
        
        if data.get('save_sequences'):
            sequences = session.get('sequences', [])
            total_items += len(sequences)
            if duplicate_sequences > 0:
                message_parts.append(f'{duplicate_sequences} duplicate sequences skipped')
        
        if data.get('save_consensus'):
            consensus_results = session.get('consensus_results', [])
            total_items += len(consensus_results)
            if duplicate_consensus > 0:
                message_parts.append(f'{duplicate_consensus} duplicate consensus sequences skipped')
        
        base_message = f'Successfully saved {saved_count} items to database'
        if message_parts:
            base_message += f' ({" + ".join(message_parts)})'
        
        # Emit completion
        if socketio:
            socketio.emit('save_operation_completed', {
                'success': True,
                'message': base_message,
                'saved_count': saved_count,
                'total_processed': total_items,
                'duplicates_skipped': total_items - saved_count,
                'timestamp': datetime.datetime.now().isoformat()
            })
        
        return jsonify({
            'success': True,
            'message': base_message,
            'saved_count': saved_count,
            'total_processed': total_items,
            'duplicates_skipped': total_items - saved_count,
            'realtime_enabled': True
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        
        # Emit error
        socketio = current_app.socketio if hasattr(current_app, 'socketio') else None
        if socketio:
            socketio.emit('save_operation_completed', {
                'success': False,
                'message': f'Failed to save to database: {str(e)}',
                'timestamp': datetime.datetime.now().isoformat()
            })
        
        return jsonify({
            'success': False,
            'message': f'Failed to save to database: {str(e)}',
            'realtime_enabled': socketio is not None
        }), 500


def background_cleanup(session_id, upload_folder, socketio):
    """Background cleanup of AB1 files to avoid blocking the main thread"""
    try:
        if session_id:
            session_folder = os.path.join(upload_folder, 'ab1_sessions', session_id)
            
            if os.path.exists(session_folder):
                import shutil
                # Remove all AB1 files from the session folder
                ab1_files_removed = 0
                total_files = len([f for f in os.listdir(session_folder) 
                                if f.lower().endswith(('.ab1', '.abi', '.scf'))])
                
                # Send initial progress
                if socketio:
                    socketio.emit('save_progress', {
                        'current': 0,
                        'total': max(1, total_files),  # Avoid division by zero
                        'type': 'cleanup',
                        'message': f'Cleaning up {total_files} temporary files...'
                    })
                
                for filename in os.listdir(session_folder):
                    if filename.lower().endswith(('.ab1', '.abi', '.scf')):
                        filepath = os.path.join(session_folder, filename)
                        try:
                            os.remove(filepath)
                            ab1_files_removed += 1
                            print(f"[CLEANUP] Removed AB1 file: {filename}")
                            
                            # Send progress update
                            if socketio:
                                socketio.emit('save_progress', {
                                    'current': ab1_files_removed,
                                    'total': max(1, total_files),
                                    'type': 'cleanup',
                                    'message': f'Cleaned up {ab1_files_removed}/{total_files} files...'
                                })
                        except Exception as e:
                            print(f"[CLEANUP] Failed to remove {filename}: {e}")
                
                # If folder is empty after cleanup, remove the entire session folder
                try:
                    if not os.listdir(session_folder):
                        shutil.rmtree(session_folder)
                        print(f"[CLEANUP] Removed empty session folder: {session_id}")
                except Exception as e:
                    print(f"[CLEANUP] Failed to remove session folder: {e}")
                
                print(f"[CLEANUP] AB1 cleanup completed. Removed {ab1_files_removed} files from session {session_id}")
                
                # Emit cleanup completion
                if socketio:
                    socketio.emit('cleanup_completed', {
                        'files_removed': ab1_files_removed,
                        'session_id': session_id,
                        'timestamp': datetime.datetime.now().isoformat()
                    })
            else:
                print(f"[CLEANUP] Session folder does not exist: {session_folder}")
                if socketio:
                    socketio.emit('cleanup_completed', {
                        'files_removed': 0,
                        'session_id': session_id,
                        'timestamp': datetime.datetime.now().isoformat()
                    })
        else:
            print("[CLEANUP] No session ID provided")
            if socketio:
                socketio.emit('cleanup_completed', {
                    'files_removed': 0,
                    'session_id': None,
                    'timestamp': datetime.datetime.now().isoformat()
                })
                
    except Exception as e:
        print(f"[CLEANUP] Error during AB1 cleanup: {e}")
        import traceback
        traceback.print_exc()
        if socketio:
            socketio.emit('cleanup_error', {
                'error': str(e),
                'timestamp': datetime.datetime.now().isoformat()
            })

