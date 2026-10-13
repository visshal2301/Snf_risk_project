"""
Audit Execution Logger Module
=============================
Provides persistent audit logging capabilities to track pipeline start, success,
failure, row counts, execution duration, and warning metrics into ENG.AUDIT_EXECUTION_LOG inside Snowflake.
"""

import time


class AuditLogger:
    """Manages persistent execution logging in ENG.AUDIT_EXECUTION_LOG table."""

    def __init__(self, session, pipeline_name: str, subject_area: str, target_partition: str):
        self.session = session
        self.pipeline_name = pipeline_name
        self.subject_area = subject_area
        self.target_partition = target_partition
        self.start_time = time.time()
        self.log_id = None
        self._initialize_audit_record()

    def _initialize_audit_record(self):
        """Inserts a new record into ENG.AUDIT_EXECUTION_LOG with STATUS='STARTED'."""
        try:
            sql = f"""
            INSERT INTO ENG.AUDIT_EXECUTION_LOG (
                PIPELINE_NAME,
                SUBJECT_AREA,
                TARGET_PARTITION,
                STATUS,
                CREATED_AT
            )
            VALUES (
                '{self.pipeline_name}',
                '{self.subject_area}',
                '{self.target_partition}',
                'STARTED',
                CURRENT_TIMESTAMP()
            )
            """
            self.session.sql(sql).collect()
            
            # Retrieve the auto-generated LOG_ID
            self.log_id = self.session.sql(
                f"""
                SELECT MAX(LOG_ID) 
                FROM ENG.AUDIT_EXECUTION_LOG 
                WHERE PIPELINE_NAME = '{self.pipeline_name}' 
                  AND SUBJECT_AREA = '{self.subject_area}'
                  AND TARGET_PARTITION = '{self.target_partition}'
                """
            ).collect()[0][0]
        except Exception as e:
            print(f"[AUDIT LOG WARNING] Failed to initialize audit record: {str(e)}")

    def log_success(
        self,
        staged_rows: int = 0,
        loaded_rows: int = 0,
        deleted_rows: int = 0,
        expired_rows: int = 0,
        warning_count: int = 0
    ):
        """Updates the audit log entry to STATUS='SUCCESS' with row counts and duration."""
        if not self.log_id:
            return

        duration_sec = round(time.time() - self.start_time, 2)
        try:
            sql = f"""
            UPDATE ENG.AUDIT_EXECUTION_LOG
            SET 
                STATUS = 'SUCCESS',
                STAGED_ROW_COUNT = {staged_rows},
                LOADED_ROW_COUNT = {loaded_rows},
                DELETED_ROW_COUNT = {deleted_rows},
                EXPIRED_ROW_COUNT = {expired_rows},
                WARNING_COUNT = {warning_count},
                EXECUTION_DURATION_SEC = {duration_sec},
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE LOG_ID = {self.log_id}
            """
            self.session.sql(sql).collect()
        except Exception as e:
            print(f"[AUDIT LOG WARNING] Failed to log success metrics: {str(e)}")

    def log_failure(self, error_message: str):
        """Updates the audit log entry to STATUS='FAILED' with error details."""
        if not self.log_id:
            return

        duration_sec = round(time.time() - self.start_time, 2)
        clean_error = str(error_message).replace("'", "''")[:950]
        try:
            sql = f"""
            UPDATE ENG.AUDIT_EXECUTION_LOG
            SET 
                STATUS = 'FAILED',
                ERROR_MESSAGE = '{clean_error}',
                EXECUTION_DURATION_SEC = {duration_sec},
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE LOG_ID = {self.log_id}
            """
            self.session.sql(sql).collect()
        except Exception as e:
            print(f"[AUDIT LOG WARNING] Failed to log failure metrics: {str(e)}")
