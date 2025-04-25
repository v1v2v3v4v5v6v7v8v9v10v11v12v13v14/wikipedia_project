"""
Standardized error handling and reporting utilities
"""

# TODO: Integrate ErrorHandler into parser methods as follows:
# stats = {'error_count': 0, 'error_types': {}}
# error_handler = ErrorHandler(logger, stats, debug=True)
#
# try:
#     # Example parsing logic (replace with actual parsing code)
#     parts = line.strip().split('\t')
#     page_title = parts[0]
#     views = int(parts[1])
#
#     # Return or process parsed data as needed
#
# except Exception as e:
#     context = ErrorHandler.create_context(
#         component="YourParserName.method_name",
#         item_id="parsed_item_id",
#         metadata={"line_preview": line[:50]}
#     )
#     error_handler.handle(e, context=context)


import logging
import traceback
from datetime import datetime, timezone
from typing import Dict, Any, Optional

class ErrorHandler:
    """
    Centralized error handling with stats tracking and context logging
    """
    
    def __init__(
        self,
        logger: logging.Logger,
        stats: Optional[Dict[str, Any]] = None,
        debug: bool = False
    ):
        """
        Args:
            logger: Configured logger instance
            stats: Optional stats dictionary to update
            debug: Enable detailed error reporting
        """
        self.logger = logger
        self.stats = stats or {}
        self.debug = debug
        
        # Initialize stats if empty
        self.stats.setdefault('error_count', 0)
        self.stats.setdefault('error_types', {})
        self.stats.setdefault('last_error', None)

    def handle(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]] = None,
        increment_stats: bool = True
    ) -> Dict[str, Any]:
        """
        Process an exception and return updated stats
        
        Args:
            error: Exception to handle
            context: Additional context about the error
            increment_stats: Whether to update error counters
            
        Returns:
            Updated stats dictionary
        """
        error_type = type(error).__name__
        error_details = self._build_error_details(error, context, error_type)
        
        if increment_stats:
            self._update_stats(error_type, error_details)
            
        self._log_error(error, error_details)
        return self.stats

    def _build_error_details(
        self,
        error: Exception,
        context: Optional[Dict[str, Any]],
        error_type: str
    ) -> Dict[str, Any]:
        """Construct detailed error information dictionary"""
        return {
            'type': error_type,
            'message': str(error),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'context': context,
            'traceback': traceback.format_exc() if self.debug else None
        }

    def _update_stats(self, error_type: str, error_details: Dict[str, Any]) -> None:
        """Update error statistics with the given error details"""
        self.stats['error_count'] += 1
        self.stats['error_types'][error_type] = self.stats['error_types'].get(error_type, 0) + 1
        self.stats['last_error'] = error_details  # Now properly using the passed error_details

    def _log_error(self, error: Exception, details: Dict[str, Any]) -> None:
        """Log error with appropriate level"""
        if isinstance(error, (KeyboardInterrupt, SystemExit)):
            self.logger.critical("Process interrupted: %s", details)
        else:
            log_method = self.logger.error if not self.debug else self.logger.exception
            log_method("Error occurred: %s", details)

    @classmethod
    def create_context(
        cls,
        component: str,
        item_id: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """
        Create standardized error context
        
        Args:
            component: Which component failed
            item_id: ID of item being processed
            metadata: Additional context
            
        Returns:
            Context dictionary
        """
        return {
            'component': component,
            'item_id': item_id,
            'metadata': metadata or {}
        }