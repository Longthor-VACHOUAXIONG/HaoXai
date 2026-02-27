/**
 * Real-time updates client for ViroDB
 * Handles WebSocket connections and real-time data updates
 */

class ViroDBRealtime {
    constructor() {
        this.socket = null;
        this.isConnected = false;
        this.subscribers = new Map();
        this.init();
    }

    init() {
        // Connect to Socket.IO server
        this.socket = io({
            reconnection: true,
            reconnectionAttempts: 5,
            reconnectionDelay: 1000,
            reconnectionDelayMax: 5000,
            timeout: 20000
        });
        
        this.socket.on('connect', () => {
            this.isConnected = true;
            console.log('Connected to ViroDB real-time updates');
            
            // Subscribe to general updates
            this.subscribe('all');
            
            // Clear any connection error notifications
            this.clearConnectionErrorNotification();
        });

        this.socket.on('disconnect', (reason) => {
            this.isConnected = false;
            console.log('Disconnected from real-time updates:', reason);
            
            if (reason === 'io server disconnect') {
                // Server initiated disconnect, need to reconnect manually
                this.socket.connect();
            }
            
            this.showNotification('Disconnected from real-time updates', 'warning');
            this.showConnectionErrorNotification();
        });

        this.socket.on('connect_error', (error) => {
            console.error('Connection error:', error);
            this.showConnectionErrorNotification();
        });

        this.socket.on('reconnect', (attemptNumber) => {
            console.log(`Reconnected to ViroDB after ${attemptNumber} attempts`);
            this.showNotification('Reconnected to real-time updates', 'success', 3000);
            this.clearConnectionErrorNotification();
        });

        this.socket.on('reconnect_attempt', (attemptNumber) => {
            console.log(`Attempting to reconnect... (${attemptNumber})`);
        });

        this.socket.on('reconnect_failed', () => {
            console.error('Failed to reconnect to server');
            this.showNotification('Failed to reconnect to server. Please refresh the page.', 'danger', 8000);
        });

        // Handle real-time events
        this.socket.on('data_inserted', (data) => {
            this.handleDataInserted(data);
        });

        this.socket.on('links_created', (data) => {
            this.handleLinksCreated(data);
        });

        this.socket.on('trigger_activated', (data) => {
            this.handleTriggerActivated(data);
        });

        this.socket.on('stats_updated', (data) => {
            this.handleStatsUpdated(data);
        });

        this.socket.on('data_updated', (data) => {
            this.handleDataUpdated(data);
        });

        this.socket.on('data_deleted', (data) => {
            this.handleDataDeleted(data);
        });

        this.socket.on('database_updated', (data) => {
            this.handleDatabaseUpdated(data);
        });

        this.socket.on('save_operation_started', (data) => {
            this.handleSaveOperationStarted(data);
        });

        this.socket.on('save_progress', (data) => {
            this.handleSaveProgress(data);
        });

        this.socket.on('save_operation_completed', (data) => {
            this.handleSaveOperationCompleted(data);
        });

        this.socket.on('cleanup_completed', (data) => {
            this.handleCleanupCompleted(data);
        });

        this.socket.on('cleanup_error', (data) => {
            this.handleCleanupError(data);
        });

        this.socket.on('status', (data) => {
            console.log('Status:', data.msg);
        });
    }

    subscribe(table) {
        if (this.isConnected) {
            this.socket.emit('subscribe_updates', { table: table });
        }
    }

    handleDataInserted(data) {
        console.log('New data inserted:', data);
        
        // Update table counts if on dashboard
        if (window.location.pathname === '/main/dashboard') {
            this.updateTableStats();
        }
        
        // Show notification
        this.showNotification(
            `New data added to ${data.table} (${data.count} records)`,
            'info',
            3000
        );
        
        // Refresh current table content dynamically if it's showing the affected table
        if (window.location.pathname.includes(data.table)) {
            this.refreshTableContent(data.table);
        }
    }

    handleLinksCreated(data) {
        console.log('Links created:', data);
        
        // Show detailed notification
        let message = `${data.count} links created in ${data.link_table}`;
        if (data.trigger_created) {
            message += ' (with automatic triggers)';
        }
        
        this.showNotification(message, 'success', 5000);
        
        // Update link statistics if on auto-linking page
        if (window.location.pathname === '/auto-link/') {
            this.updateLinkStats();
        }
        
        // Update dashboard stats
        if (window.location.pathname === '/main/dashboard') {
            this.updateTableStats();
        }
    }

    handleTriggerActivated(data) {
        console.log('Trigger activated:', data);
        
        this.showNotification(
            `Auto-link trigger fired: ${data.trigger_name}`,
            'success',
            4000
        );
        
        // Update statistics
        this.updateLinkStats();
        this.updateTableStats();
    }

    handleStatsUpdated(data) {
        console.log('Stats updated:', data);
        
        // Update any statistics displays
        this.updateTableStats();
        this.updateLinkStats();
    }

    handleDataUpdated(data) {
        console.log('Data updated:', data);
        
        // Show notification
        this.showNotification(
            `Data updated in ${data.table} (${data.count} records)`,
            'info',
            3000
        );
        
        // Refresh current table content dynamically if it's showing the affected table
        if (window.location.pathname.includes(data.table)) {
            this.refreshTableContent(data.table);
        }
        
        // Update dashboard stats
        if (window.location.pathname === '/main/dashboard') {
            this.updateTableStats();
        }
    }

    handleDataDeleted(data) {
        console.log('Data deleted:', data);
        
        // Show notification
        this.showNotification(
            `Data deleted from ${data.table} (${data.count} records)`,
            'warning',
            4000
        );
        
        // Refresh current table content dynamically if it's showing the affected table
        if (window.location.pathname.includes(data.table)) {
            this.refreshTableContent(data.table);
        }
        
        // Update dashboard stats
        if (window.location.pathname === '/main/dashboard') {
            this.updateTableStats();
        }
    }

    handleDatabaseUpdated(data) {
        console.log('Database updated:', data);
        
        // Show general notification
        this.showNotification(
            `Database updated: ${data.action} on ${data.table}`,
            'info',
            3000
        );
        
        // Update all relevant displays
        this.updateTableStats();
        this.updateLinkStats();
        
        // Refresh current table if needed
        if (window.location.pathname.includes(data.table)) {
            this.refreshTableContent(data.table);
        }
    }

    handleSaveOperationStarted(data) {
        console.log('Save operation started:', data);
        
        // Show progress notification
        this.showSaveProgress(data.message, 0, 100);
        
        // Disable save buttons during operation
        document.querySelectorAll('button[data-action="save"]').forEach(btn => {
            btn.disabled = true;
            btn.dataset.originalText = btn.innerHTML;
            btn.innerHTML = '<span class="spinner-border spinner-border-sm me-2"></span>Saving...';
        });
    }

    handleSaveProgress(data) {
        console.log('Save progress:', data);
        
        // Update progress display
        const percentage = Math.round((data.current / data.total) * 100);
        this.showSaveProgress(data.message, percentage, 100);
        
        // Update progress bar if exists
        const progressBar = document.querySelector('#save-progress-bar');
        if (progressBar) {
            progressBar.style.width = `${percentage}%`;
            progressBar.setAttribute('aria-valuenow', percentage);
        }
        
        const progressText = document.querySelector('#save-progress-text');
        if (progressText) {
            progressText.textContent = `${data.current}/${data.total} ${data.type} processed`;
        }
    }

    handleSaveOperationCompleted(data) {
        console.log('Save operation completed:', data);
        
        // Re-enable save buttons
        document.querySelectorAll('button[data-action="save"]').forEach(btn => {
            btn.disabled = false;
            btn.innerHTML = btn.dataset.originalText || 'Save';
        });
        
        if (data.success) {
            // Show success notification
            this.showNotification(data.message, 'success', 5000);
            
            // Hide progress display
            this.hideSaveProgress();
            
            // Update dashboard if on dashboard page
            if (window.location.pathname === '/main/dashboard') {
                this.updateTableStats();
            }
            
            // Update any relevant tables
            if (data.saved_count > 0) {
                ['sequences', 'consensus_sequences', 'blast_results', 'projects'].forEach(table => {
                    if (window.location.pathname.includes(table)) {
                        this.refreshTableContent(table);
                    }
                });
            }
        } else {
            // Show error notification
            this.showNotification(data.message, 'danger', 8000);
            this.hideSaveProgress();
        }
    }

    handleCleanupCompleted(data) {
        console.log('Cleanup completed:', data);
        
        // Hide progress display
        this.hideSaveProgress();
        
        // Show brief cleanup notification
        this.showNotification(
            `Cleanup completed: ${data.files_removed} temporary files removed`,
            'info',
            3000
        );
    }

    handleCleanupError(data) {
        console.log('Cleanup error:', data);
        
        // Hide progress display
        this.hideSaveProgress();
        
        // Show cleanup error notification
        this.showNotification(
            `Cleanup warning: ${data.error}`,
            'warning',
            4000
        );
    }

    showSaveProgress(message, percentage, total) {
        // Create or update progress modal
        let progressModal = document.getElementById('save-progress-modal');
        
        if (!progressModal) {
            progressModal = document.createElement('div');
            progressModal.id = 'save-progress-modal';
            progressModal.className = 'modal fade';
            progressModal.innerHTML = `
                <div class="modal-dialog modal-dialog-centered">
                    <div class="modal-content">
                        <div class="modal-header bg-primary text-white">
                            <h5 class="modal-title">
                                <i class="bi bi-database me-2"></i>Saving to Database
                            </h5>
                        </div>
                        <div class="modal-body">
                            <p id="save-progress-message">${message}</p>
                            <div class="progress mb-2">
                                <div id="save-progress-bar" class="progress-bar progress-bar-striped progress-bar-animated" 
                                     role="progressbar" style="width: 0%" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100">
                                </div>
                            </ </div>
                            <small id="save-progress-text" class="text-muted">0/0 items processed</small>
                        </div>
                        <div class="modal-footer">
                            <small class="text-muted">
                                <i class="bi bi-info-circle me-1"></i>
                                Please wait - this may take a few moments
                            </small>
                        </div>
                    </div>
                </div>
            `;
            document.body.appendChild(progressModal);
        }
        
        // Update progress
        const messageEl = document.getElementById('save-progress-message');
        const progressBar = document.getElementById('save-progress-bar');
        const progressText = document.getElementById('save-progress-text');
        
        if (messageEl) messageEl.textContent = message;
        if (progressBar) {
            progressBar.style.width = `${percentage}%`;
            progressBar.setAttribute('aria-valuenow', percentage);
        }
        if (progressText) progressText.textContent = `${Math.round(percentage)}% complete`;
        
        // Show modal if not already visible
        const modal = bootstrap.Modal.getInstance(progressModal);
        if (!modal) {
            const newModal = new bootstrap.Modal(progressModal, {
                backdrop: 'static',
                keyboard: false
            });
            newModal.show();
        }
    }

    hideSaveProgress() {
        const progressModal = document.getElementById('save-progress-modal');
        if (progressModal) {
            const modal = bootstrap.Modal.getInstance(progressModal);
            if (modal) {
                modal.hide();
            }
            
            // Remove modal after hidden animation
            progressModal.addEventListener('hidden.bs.modal', () => {
                progressModal.remove();
            }, { once: true });
        }
    }

    updateTableStats() {
        // Refresh dashboard statistics
        if (typeof loadDashboardStats === 'function') {
            loadDashboardStats();
        }
        
        // Update any stat cards
        document.querySelectorAll('.stats-card').forEach(card => {
            const table = card.dataset.table;
            if (table) {
                fetch(`/main/get-table-count/${table}`)
                    .then(r => r.json())
                    .then(data => {
                        if (data.success) {
                            const countElement = card.querySelector('.count');
                            if (countElement) {
                                countElement.textContent = data.count;
                            }
                        }
                    })
                    .catch(err => console.error('Failed to update stats:', err));
            }
        });
    }

    refreshTableContent(tableName) {
        // Dynamically refresh table content without page reload
        const tableContainer = document.querySelector('.table-container, .data-table-container, #table-content');
        if (tableContainer) {
            fetch(`/database/view-table/${tableName}?partial=true`)
                .then(response => response.text())
                .then(html => {
                    if (html) {
                        // Create a temporary div to parse the response
                        const tempDiv = document.createElement('div');
                        tempDiv.innerHTML = html;
                        
                        // Find the new table content
                        const newTable = tempDiv.querySelector('table, .table-responsive');
                        if (newTable) {
                            // Replace the existing table
                            const existingTable = tableContainer.querySelector('table, .table-responsive');
                            if (existingTable) {
                                existingTable.replaceWith(newTable);
                            } else {
                                tableContainer.innerHTML = html;
                            }
                            
                            // Re-initialize any table functionality
                            this.reinitializeTableFeatures();
                        }
                    }
                })
                .catch(error => {
                    console.error('Failed to refresh table content:', error);
                    // Fallback: reload the specific table data
                    this.reloadTableData(tableName);
                });
        }
    }

    reloadTableData(tableName) {
        // Fallback method to reload just the table data
        fetch(`/database/get-table-data/${tableName}`)
            .then(response => response.json())
            .then(data => {
                if (data.success && data.data) {
                    this.updateTableWithData(data.data);
                }
            })
            .catch(error => console.error('Failed to reload table data:', error));
    }

    updateTableWithData(tableData) {
        const tableBody = document.querySelector('table tbody');
        if (tableBody && Array.isArray(tableData)) {
            // Clear existing rows
            tableBody.innerHTML = '';
            
            // Add new rows
            tableData.forEach(row => {
                const tr = document.createElement('tr');
                Object.values(row).forEach(value => {
                    const td = document.createElement('td');
                    td.textContent = value || '';
                    tr.appendChild(td);
                });
                tableBody.appendChild(tr);
            });
        }
    }

    reinitializeTableFeatures() {
        // Re-initialize sorting, pagination, and other table features
        if (typeof initializeDataTable === 'function') {
            initializeDataTable();
        }
        if (typeof initializeSorting === 'function') {
            initializeSorting();
        }
        if (typeof initializePagination === 'function') {
            initializePagination();
        }
    }

    updateLinkStats() {
        // Refresh link statistics
        if (typeof loadLinkStats === 'function') {
            loadLinkStats();
        }
    }

    showNotification(message, type = 'info', duration = 4000) {
        // Create notification element
        const notification = document.createElement('div');
        notification.className = `alert alert-${type} alert-dismissible fade show real-time-notification`;
        notification.style.cssText = `
            position: fixed;
            top: 20px;
            right: 20px;
            z-index: 9999;
            min-width: 300px;
            max-width: 500px;
            animation: slideInRight 0.3s ease-out;
        `;
        
        notification.innerHTML = `
            <div class="d-flex align-items-center">
                <div class="flex-grow-1">
                    <i class="fas fa-bolt me-2"></i>
                    <strong>Real-time:</strong> ${message}
                </div>
                <button type="button" class="btn-close btn-close-white ms-2" data-bs-dismiss="alert"></button>
            </div>
        `;
        
        // Add to page
        document.body.appendChild(notification);
        
        // Auto-remove after duration
        setTimeout(() => {
            if (notification.parentNode) {
                notification.style.animation = 'slideOutRight 0.3s ease-in';
                setTimeout(() => {
                    if (notification.parentNode) {
                        notification.parentNode.removeChild(notification);
                    }
                }, 300);
            }
        }, duration);
        
        // Handle manual close
        notification.querySelector('.btn-close').addEventListener('click', () => {
            notification.style.animation = 'slideOutRight 0.3s ease-in';
            setTimeout(() => {
                if (notification.parentNode) {
                    notification.parentNode.removeChild(notification);
                }
            }, 300);
        });
    }

    showConnectionErrorNotification() {
        // Remove existing connection error notification
        this.clearConnectionErrorNotification();
        
        const notification = document.createElement('div');
        notification.id = 'connection-error-notification';
        notification.className = 'alert alert-danger alert-dismissible fade show real-time-notification';
        notification.style.cssText = `
            position: fixed;
            top: 80px;
            right: 20px;
            z-index: 9998;
            min-width: 350px;
            max-width: 500px;
            animation: slideInRight 0.3s ease-out;
        `;
        
        notification.innerHTML = `
            <div class="d-flex align-items-center">
                <div class="flex-grow-1">
                    <i class="fas fa-exclamation-triangle me-2"></i>
                    <strong>Connection Lost:</strong> Real-time updates disconnected
                    <br><small class="text-muted">Attempting to reconnect automatically...</small>
                </div>
                <button type="button" class="btn-close btn-close-white ms-2" data-bs-dismiss="alert"></button>
            </div>
        `;
        
        document.body.appendChild(notification);
    }

    clearConnectionErrorNotification() {
        const existingNotification = document.getElementById('connection-error-notification');
        if (existingNotification) {
            existingNotification.style.animation = 'slideOutRight 0.3s ease-in';
            setTimeout(() => {
                if (existingNotification.parentNode) {
                    existingNotification.parentNode.removeChild(existingNotification);
                }
            }, 300);
        }
    }
}

// Add CSS animations
const style = document.createElement('style');
style.textContent = `
    @keyframes slideInRight {
        from {
            transform: translateX(100%);
            opacity: 0;
        }
        to {
            transform: translateX(0);
            opacity: 1;
        }
    }
    
    @keyframes slideOutRight {
        from {
            transform: translateX(0);
            opacity: 1;
        }
        to {
            transform: translateX(100%);
            opacity: 0;
        }
    }
    
    .real-time-notification {
        box-shadow: 0 4px 12px rgba(0,0,0,0.15);
        border: none;
        border-left: 4px solid;
    }
    
    .real-time-notification.alert-info {
        border-left-color: #0dcaf0;
    }
    
    .real-time-notification.alert-success {
        border-left-color: #198754;
    }
    
    .real-time-notification.alert-warning {
        border-left-color: #ffc107;
    }
`;
document.head.appendChild(style);

// Initialize real-time client when page loads
let realtimeClient;
document.addEventListener('DOMContentLoaded', () => {
    realtimeClient = new ViroDBRealtime();
});

// Make it globally available
window.ViroDBRealtime = ViroDBRealtime;
window.realtimeClient = realtimeClient;
