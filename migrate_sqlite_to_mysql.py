from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

import pymysql

import app


TABLES = {
    "phone_list_entries": (
        "id",
        "dsp_key",
        "label",
        "last_name",
        "work_phone",
        "home_phone",
        "mobile_phone",
        "updated_at",
    ),
    "transporter_id_entries": (
        "id",
        "dsp_key",
        "da_key",
        "da_name",
        "transporter_id",
        "notes",
        "updated_at",
    ),
    "associate_data_entries": ("id", "dsp_key", "raw_payload", "updated_at"),
    "vehicle_data_entries": ("id", "dsp_key", "raw_payload", "updated_at"),
    "activity_logs": (
        "id",
        "iso_time",
        "timestamp",
        "action",
        "details",
        "account_key",
        "account_name",
        "session_id",
        "current_tab",
        "page",
        "user_agent",
        "ip_address",
        "raw_payload",
        "created_at",
    ),
    "portkey_attendance_report": ("id", "raw_payload", "updated_at"),
    "user_accounts": (
        "id",
        "username",
        "display_name",
        "password_hash",
        "dsp_key",
        "dsp_keys",
        "profile_picture",
        "is_active",
        "must_change_password",
        "created_at",
        "updated_at",
        "last_login_at",
    ),
    "account_audit_logs": (
        "id",
        "user_id",
        "username",
        "action",
        "details",
        "actor",
        "ip_address",
        "created_at",
    ),
}


def sqlite_table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def mysql_count(cursor, table_name: str) -> int:
    cursor.execute(f"SELECT COUNT(*) AS count FROM `{table_name}`")
    return int(cursor.fetchone()["count"])


def reset_mysql_tables(cursor) -> None:
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")
    for table_name in reversed(TABLES):
        cursor.execute(f"DELETE FROM `{table_name}`")
        cursor.execute(f"ALTER TABLE `{table_name}` AUTO_INCREMENT = 1")
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")


def copy_table(sqlite_conn: sqlite3.Connection, mysql_cursor, table_name: str, columns: tuple[str, ...]) -> int:
    if not sqlite_table_exists(sqlite_conn, table_name):
        return 0

    selected_columns = ", ".join(columns)
    rows = sqlite_conn.execute(f"SELECT {selected_columns} FROM {table_name} ORDER BY id ASC").fetchall()
    if not rows:
        return 0

    mysql_columns = ", ".join(f"`{column}`" for column in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    mysql_cursor.executemany(
        f"INSERT INTO `{table_name}` ({mysql_columns}) VALUES ({placeholders})",
        [tuple(row[column] for column in columns) for row in rows],
    )

    mysql_cursor.execute(f"SELECT COALESCE(MAX(id), 0) + 1 AS next_id FROM `{table_name}`")
    next_id = int(mysql_cursor.fetchone()["next_id"])
    mysql_cursor.execute(f"ALTER TABLE `{table_name}` AUTO_INCREMENT = {next_id}")
    return len(rows)


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy dispatch tool data from SQLite to Railway MySQL.")
    parser.add_argument(
        "--sqlite",
        default=str(app.DB_FILE),
        help="Path to the SQLite database file. Defaults to the app's current DB path.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing MySQL rows before copying SQLite data.",
    )
    args = parser.parse_args()

    sqlite_path = Path(args.sqlite).expanduser()
    if not sqlite_path.exists():
        print(f"SQLite database not found: {sqlite_path}", file=sys.stderr)
        return 1
    if not app.USE_MYSQL:
        print("Set MYSQL_URL or MYSQLHOST/MYSQLPORT/MYSQLUSER/MYSQLPASSWORD/MYSQLDATABASE first.", file=sys.stderr)
        return 1

    app.init_db()

    sqlite_conn = sqlite3.connect(sqlite_path)
    sqlite_conn.row_factory = sqlite3.Row

    mysql_conn = pymysql.connect(
        **app.get_mysql_config(),
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )

    try:
        with mysql_conn.cursor() as cursor:
            existing_rows = sum(mysql_count(cursor, table_name) for table_name in TABLES)
            if existing_rows and not args.replace:
                print(
                    f"MySQL already has {existing_rows} rows. Re-run with --replace to overwrite them.",
                    file=sys.stderr,
                )
                return 1

            if args.replace:
                reset_mysql_tables(cursor)

            copied = {}
            for table_name, columns in TABLES.items():
                copied[table_name] = copy_table(sqlite_conn, cursor, table_name, columns)

        mysql_conn.commit()
    except Exception:
        mysql_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        mysql_conn.close()

    print("SQLite to MySQL migration complete.")
    for table_name, count in copied.items():
        print(f"{table_name}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
