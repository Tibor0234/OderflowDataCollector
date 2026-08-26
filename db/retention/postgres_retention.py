import os
import psycopg

from db.retention.base_retention import BaseRetention


class PostgresRetention(BaseRetention):

    def __init__(
        self,
        conn: psycopg.Connection,
        max_sessions: int
    ):
        super().__init__(conn, max_sessions)


    def get_session_count(self):

        with self.conn.cursor() as cursor:

            cursor.execute("""
                SELECT COUNT(*)
                FROM sessions
            """)

            return cursor.fetchone()[0]


    def get_oldest_session(self):

        with self.conn.cursor() as cursor:

            cursor.execute("""
                SELECT id
                FROM sessions
                ORDER BY created_at ASC
                LIMIT 1
            """)

            row = cursor.fetchone()

            return row[0] if row else None


    def delete_session(self, session_id):

        log_paths = []

        try:

            with self.conn.cursor() as cursor:

                # --------------------------------------------------
                # Get session pairs
                # --------------------------------------------------

                cursor.execute("""
                    SELECT id
                    FROM session_pairs
                    WHERE session_id = %s
                """, (session_id,))

                pair_ids = [
                    row[0]
                    for row in cursor.fetchall()
                ]


                # --------------------------------------------------
                # Get log file paths before deleting log records
                # --------------------------------------------------

                cursor.execute("""
                    SELECT filename
                    FROM logs
                    WHERE session_id = %s
                """, (session_id,))

                log_paths = [
                    row[0]
                    for row in cursor.fetchall()
                ]


                # --------------------------------------------------
                # Delete pair-related data
                # --------------------------------------------------

                for pair_id in pair_ids:

                    cursor.execute("""
                        DELETE FROM trades
                        WHERE session_pair_id = %s
                    """, (pair_id,))

                    cursor.execute("""
                        DELETE FROM orderbooks
                        WHERE session_pair_id = %s
                    """, (pair_id,))

                    cursor.execute("""
                        DELETE FROM open_interest
                        WHERE session_pair_id = %s
                    """, (pair_id,))

                    cursor.execute("""
                        DELETE FROM news
                        WHERE session_pair_id = %s
                    """, (pair_id,))

                    cursor.execute("""
                        DELETE FROM ohlcv
                        WHERE session_pair_id = %s
                    """, (pair_id,))


                # --------------------------------------------------
                # Delete logs
                # --------------------------------------------------

                cursor.execute("""
                    DELETE FROM logs
                    WHERE session_id = %s
                """, (session_id,))


                # --------------------------------------------------
                # Delete session pairs
                # --------------------------------------------------

                cursor.execute("""
                    DELETE FROM session_pairs
                    WHERE session_id = %s
                """, (session_id,))


                # --------------------------------------------------
                # Delete session
                # --------------------------------------------------

                cursor.execute("""
                    DELETE FROM sessions
                    WHERE id = %s
                """, (session_id,))


            # ------------------------------------------------------
            # Commit the entire database operation
            # ------------------------------------------------------

            self.conn.commit()


        except Exception:

            self.conn.rollback()

            raise


        # ----------------------------------------------------------
        # Delete physical log files only AFTER successful commit
        # ----------------------------------------------------------

        for path in log_paths:

            try:

                if os.path.isfile(path):
                    os.remove(path)

            except OSError as exc:

                print(
                    f"Warning: failed to delete log file "
                    f"{path}: {exc}"
                )

    def vacuum(self):

        previous_autocommit = self.conn.autocommit

        try:
            self.conn.autocommit = True

            with self.conn.cursor() as cursor:

                self.logger.info(
                    "Starting VACUUM on trades and orderbooks."
                )

                cursor.execute("""
                    VACUUM ANALYZE trades
                """)

                cursor.execute("""
                    VACUUM ANALYZE orderbooks
                """)

                self.logger.info(
                    "VACUUM completed successfully."
                )

        except Exception:
            self.logger.exception(
                "VACUUM failed."
            )
            raise

        finally:
            self.conn.autocommit = previous_autocommit