"""
Real-time updates helper for ViroDB
Provides easy methods to send real-time updates via SocketIO
"""

def emit_realtime_update(socketio, event_type, data, room=None):
    """
    Emit a real-time update to connected clients
    
    Args:
        socketio: SocketIO instance
        event_type: Type of event ('data_inserted', 'links_created', 'trigger_activated', etc.)
        data: Dictionary with update data
        room: Optional room to broadcast to (for targeted updates)
    """
    try:
        if room:
            socketio.emit(event_type, data, room=room)
        else:
            socketio.emit(event_type, data)
        print(f"[REALTIME] Emitted {event_type}: {data}")
    except Exception as e:
        print(f"[ERROR] Failed to emit real-time update: {e}")

def notify_data_insertion(socketio, table_name, record_count=1, source_id=None):
    """Notify clients about new data insertion"""
    data = {
        'table': table_name,
        'count': record_count,
        'source_id': source_id,
        'timestamp': str(datetime.datetime.now())
    }
    emit_realtime_update(socketio, 'data_inserted', data)

def notify_links_created(socketio, link_table, count, trigger_created=False):
    """Notify clients about new links being created"""
    data = {
        'link_table': link_table,
        'count': count,
        'trigger_created': trigger_created,
        'timestamp': str(datetime.datetime.now())
    }
    emit_realtime_update(socketio, 'links_created', data)

def notify_trigger_activated(socketio, trigger_name, source_table, target_id):
    """Notify clients about automatic trigger activation"""
    data = {
        'trigger_name': trigger_name,
        'source_table': source_table,
        'target_id': target_id,
        'timestamp': str(datetime.datetime.now())
    }
    emit_realtime_update(socketio, 'trigger_activated', data)

def notify_database_stats(socketio, stats):
    """Notify clients about updated database statistics"""
    emit_realtime_update(socketio, 'stats_updated', stats)

# Import datetime for timestamp generation
import datetime
