#!/usr/bin/env python3
"""
Kiro 在线发卡平台
支持CDK提卡、账号校验、质保替换功能
"""

import io
import json
import hashlib
import os
import random
import re
import secrets
import string
import time
from datetime import datetime, timedelta, timezone
from functools import wraps
from typing import Any, Optional
from urllib.parse import parse_qsl, unquote, urlparse
from uuid import uuid4

import psycopg2
import psycopg2.extras
import pymysql
import pymysql.cursors
from dbutils.pooled_db import PooledDB

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, Response, send_file, session, stream_with_context, url_for

# 后端统一使用 UTC，前端展示时再转北京时间
UTC_TZ = timezone.utc
BEIJING_TZ = timezone(timedelta(hours=8))


def utc_now():
    """获取 UTC 当前时间"""
    return datetime.now(UTC_TZ)


def beijing_now():
    """获取北京时间"""
    return utc_now().astimezone(BEIJING_TZ)


def to_utc_time(dt_value):
    """将数据库/字符串时间统一转换为带 UTC 时区的 datetime"""
    if not dt_value:
        return None

    if isinstance(dt_value, datetime):
        dt = dt_value
    else:
        dt = datetime.fromisoformat(str(dt_value).replace("Z", "+00:00"))

    if dt.tzinfo is None:
        # 数据库会话已设置 time_zone='+00:00'，所有无时区信息的值均为 UTC
        return dt.replace(tzinfo=UTC_TZ)
    return dt.astimezone(UTC_TZ)


def to_beijing_time(dt_value):
    """将时间转换为带北京时区的 datetime 对象，仅用于展示"""
    utc_dt = to_utc_time(dt_value)
    return utc_dt.astimezone(BEIJING_TZ) if utc_dt else None


def to_db_datetime(dt_value):
    """格式化为 MySQL DATETIME/TIMESTAMP 可接受的 UTC 时间字符串"""
    utc_dt = to_utc_time(dt_value)
    return utc_dt.strftime("%Y-%m-%d %H:%M:%S") if utc_dt else None


def to_api_datetime(dt_value):
    """返回给前端的时间统一使用带时区的 UTC ISO 字符串"""
    utc_dt = to_utc_time(dt_value)
    return utc_dt.isoformat() if utc_dt else None

def _env_str(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value not in (None, "") else default


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw in (None, ""):
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


# 默认配置（可被环境变量覆盖）
CONFIG = {
    "server": {
        "host": "0.0.0.0",
        "port": 2158,
        "debug": False,
        "secret_key": "change-me",
    },
    "admin": {
        "username": "admin",
        "password": "admin123",
    },
    "warranty": {
        "default_days": 7,
        "max_replacements": 3,
    },
    "verification": {
        "idc_region": "us-east-1",
        "profile_arn": "arn:aws:q::aws:profile/default",
        "timeout_seconds": 20,
    },
    "database": {
        "url": "mysql://root:password@localhost:3306/railway",
    },
    "cdk": {
        "length": 16,
        "prefix": "KIRO",
    },
    "warmup": {
        "prompt": "hi",
        "timeout_seconds": 30,
    },
}

# 环境变量覆盖（Railway 部署优先使用这些）
CONFIG["server"]["host"] = _env_str("HOST", str(CONFIG["server"]["host"]))
CONFIG["server"]["port"] = _env_int("PORT", int(CONFIG["server"]["port"]))
CONFIG["server"]["debug"] = _env_bool("DEBUG", bool(CONFIG["server"]["debug"]))
CONFIG["server"]["secret_key"] = _env_str("SECRET_KEY", str(CONFIG["server"]["secret_key"]))

CONFIG["admin"]["username"] = _env_str("ADMIN_USERNAME", str(CONFIG["admin"]["username"]))
CONFIG["admin"]["password"] = _env_str("ADMIN_PASSWORD", str(CONFIG["admin"]["password"]))

CONFIG["warranty"]["default_days"] = _env_int(
    "WARRANTY_DEFAULT_DAYS", int(CONFIG["warranty"]["default_days"])
)
CONFIG["warranty"]["max_replacements"] = _env_int(
    "WARRANTY_MAX_REPLACEMENTS", int(CONFIG["warranty"]["max_replacements"])
)

CONFIG["verification"]["idc_region"] = _env_str(
    "VERIFICATION_IDC_REGION", str(CONFIG["verification"]["idc_region"])
)
CONFIG["verification"]["profile_arn"] = _env_str(
    "VERIFICATION_PROFILE_ARN", str(CONFIG["verification"]["profile_arn"])
)
CONFIG["verification"]["timeout_seconds"] = _env_int(
    "VERIFICATION_TIMEOUT_SECONDS", int(CONFIG["verification"]["timeout_seconds"])
)

CONFIG["database"]["url"] = _env_str("DATABASE_URL", str(CONFIG["database"]["url"]))

CONFIG["cdk"]["length"] = _env_int("CDK_LENGTH", int(CONFIG["cdk"]["length"]))
CONFIG["cdk"]["prefix"] = _env_str("CDK_PREFIX", str(CONFIG["cdk"]["prefix"]))

CONFIG["warmup"]["prompt"] = _env_str("WARMUP_PROMPT", str(CONFIG["warmup"]["prompt"]))
CONFIG["warmup"]["timeout_seconds"] = _env_int(
    "WARMUP_TIMEOUT_SECONDS", int(CONFIG["warmup"]["timeout_seconds"])
)

app = Flask(__name__)
app.secret_key = CONFIG["server"]["secret_key"]


def _parse_database_url(url: str) -> dict:
    """解析 DATABASE_URL 为连接参数"""
    parsed = urlparse(url)
    scheme = (parsed.scheme or "mysql").lower()
    mysql_schemes = {"mysql", "mysql+pymysql"}
    postgres_schemes = {"postgres", "postgresql", "postgresql+psycopg2"}

    if scheme in postgres_schemes:
        dialect = "postgresql"
    elif scheme in mysql_schemes:
        dialect = "mysql"
    else:
        raise ValueError(f"Unsupported DATABASE_URL scheme: {parsed.scheme or '<empty>'}")

    default_port = 5432 if dialect == "postgresql" else 3306
    query_options = {
        key: value
        for key, value in parse_qsl(parsed.query, keep_blank_values=False)
        if value != ""
    }
    return {
        "dialect": dialect,
        "host": parsed.hostname or "localhost",
        "port": parsed.port or default_port,
        "user": unquote(parsed.username or ("postgres" if dialect == "postgresql" else "root")),
        "password": unquote(parsed.password or ""),
        "database": unquote((parsed.path or "/railway").lstrip("/")),
        "options": query_options,
    }


_db_params = _parse_database_url(CONFIG["database"]["url"])
print(f"[DB] DATABASE_URL = {CONFIG['database']['url'][:30]}...", flush=True)
print(
    f"[DB] Parsed: dialect={_db_params['dialect']}, host={_db_params['host']}, port={_db_params['port']}, db={_db_params['database']}",
    flush=True,
)
_pool = None


def _get_db_dialect() -> str:
    return _db_params["dialect"]


def _is_mysql() -> bool:
    return _get_db_dialect() == "mysql"


def _is_postgres() -> bool:
    return _get_db_dialect() == "postgresql"


def _get_db_driver():
    return pymysql if _is_mysql() else psycopg2


def _get_pool_kwargs() -> dict:
    if _is_mysql():
        return {
            "host": _db_params["host"],
            "port": _db_params["port"],
            "user": _db_params["user"],
            "password": _db_params["password"],
            "database": _db_params["database"],
            "charset": "utf8mb4",
            "cursorclass": pymysql.cursors.DictCursor,
            "autocommit": False,
        }

    pg_options = _db_params.get("options", {})
    pg_kwargs = {
        "host": _db_params["host"],
        "port": _db_params["port"],
        "user": _db_params["user"],
        "password": _db_params["password"],
        "dbname": _db_params["database"],
        "cursor_factory": psycopg2.extras.RealDictCursor,
    }
    for key in ("sslmode", "options", "application_name"):
        value = pg_options.get(key)
        if value:
            pg_kwargs[key] = value
    connect_timeout = pg_options.get("connect_timeout")
    if connect_timeout:
        try:
            pg_kwargs["connect_timeout"] = int(connect_timeout)
        except ValueError:
            pass
    return pg_kwargs


def _run_connection_init(db):
    with db.cursor() as cursor:
        if _is_mysql():
            cursor.execute("SET time_zone = '+00:00'")
        else:
            cursor.execute("SET TIME ZONE 'UTC'")


def _get_integrity_error_types() -> tuple[type[Exception], ...]:
    return (pymysql.IntegrityError, psycopg2.IntegrityError)


_INTEGRITY_ERRORS = _get_integrity_error_types()


def _quote_identifier(name: str) -> str:
    quote = "`" if _is_mysql() else '"'
    return f"{quote}{name}{quote}"


def _sql_insert_default_setting() -> str:
    key_column = _quote_identifier("key")
    if _is_mysql():
        return f"INSERT IGNORE INTO settings ({key_column}, value) VALUES (%s, %s)"
    return f"INSERT INTO settings ({key_column}, value) VALUES (%s, %s) ON CONFLICT ({key_column}) DO NOTHING"


def _sql_upsert_setting() -> str:
    key_column = _quote_identifier("key")
    if _is_mysql():
        return f"INSERT INTO settings ({key_column}, value) VALUES (%s, %s) ON DUPLICATE KEY UPDATE value = %s"
    return f"INSERT INTO settings ({key_column}, value) VALUES (%s, %s) ON CONFLICT ({key_column}) DO UPDATE SET value = EXCLUDED.value"


def _sql_admin_stats_idle_accounts_by_created_date() -> str:
    if _is_mysql():
        return """SELECT DATE_FORMAT(created_date, '%%Y-%%m-%%d') as date, count
           FROM (
               SELECT DATE(created_at) as created_date, COUNT(*) as count
               FROM accounts
               WHERE status = 'available'
                 AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
               GROUP BY DATE(created_at)
           ) grouped_idle_accounts
           ORDER BY created_date ASC"""
    return """SELECT TO_CHAR(created_date, 'YYYY-MM-DD') as date, count
           FROM (
               SELECT DATE(created_at) as created_date, COUNT(*) as count
               FROM accounts
               WHERE status = 'available'
                 AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
               GROUP BY DATE(created_at)
           ) grouped_idle_accounts
           ORDER BY created_date ASC"""


def _sql_like(column: str) -> str:
    operator = "LIKE" if _is_mysql() else "ILIKE"
    return f"{column} {operator} %s"


def _get_schema_sqls() -> list[str]:
    if _is_mysql():
        return [
            """
            CREATE TABLE IF NOT EXISTS cdks (
                id INT AUTO_INCREMENT PRIMARY KEY,
                code VARCHAR(100) UNIQUE NOT NULL,
                account_id INT,
                status VARCHAR(20) DEFAULT 'unused',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                used_at TIMESTAMP NULL,
                used_by VARCHAR(100)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS accounts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                access_token TEXT,
                refresh_token TEXT,
                id_token TEXT,
                token_data LONGTEXT,
                status VARCHAR(20) DEFAULT 'available',
                last_verified_at TIMESTAMP NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS orders (
                id INT AUTO_INCREMENT PRIMARY KEY,
                cdk_code VARCHAR(100) NOT NULL,
                account_id INT NOT NULL,
                user_ip VARCHAR(50),
                warranty_days INT DEFAULT 7,
                warranty_expires_at TIMESTAMP NULL,
                replacement_count INT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (account_id) REFERENCES accounts(id),
                FOREIGN KEY (cdk_code) REFERENCES cdks(code)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS replacements (
                id INT AUTO_INCREMENT PRIMARY KEY,
                order_id INT NOT NULL,
                old_account_id INT NOT NULL,
                new_account_id INT NOT NULL,
                reason TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (order_id) REFERENCES orders(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS verifications (
                id INT AUTO_INCREMENT PRIMARY KEY,
                account_id INT NOT NULL,
                order_id INT,
                verification_type VARCHAR(20) NOT NULL,
                result VARCHAR(20) NOT NULL,
                details TEXT,
                verified_at DATETIME NOT NULL,
                FOREIGN KEY (account_id) REFERENCES accounts(id),
                FOREIGN KEY (order_id) REFERENCES orders(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
            """
            CREATE TABLE IF NOT EXISTS settings (
                `key` VARCHAR(100) PRIMARY KEY,
                value TEXT NOT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            """,
        ]
    return [
        """
        CREATE TABLE IF NOT EXISTS cdks (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            code VARCHAR(100) UNIQUE NOT NULL,
            account_id INTEGER,
            status VARCHAR(20) DEFAULT 'unused',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_at TIMESTAMP NULL,
            used_by VARCHAR(100)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            email VARCHAR(255) NOT NULL,
            access_token TEXT,
            refresh_token TEXT,
            id_token TEXT,
            token_data TEXT,
            status VARCHAR(20) DEFAULT 'available',
            last_verified_at TIMESTAMP NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            cdk_code VARCHAR(100) NOT NULL,
            account_id INTEGER NOT NULL,
            user_ip VARCHAR(50),
            warranty_days INTEGER DEFAULT 7,
            warranty_expires_at TIMESTAMP NULL,
            replacement_count INTEGER DEFAULT 0,
            status VARCHAR(20) DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            FOREIGN KEY (cdk_code) REFERENCES cdks(code)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS replacements (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            order_id INTEGER NOT NULL,
            old_account_id INTEGER NOT NULL,
            new_account_id INTEGER NOT NULL,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS verifications (
            id INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            account_id INTEGER NOT NULL,
            order_id INTEGER,
            verification_type VARCHAR(20) NOT NULL,
            result VARCHAR(20) NOT NULL,
            details TEXT,
            verified_at TIMESTAMP NOT NULL,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS settings (
            "key" VARCHAR(100) PRIMARY KEY,
            value TEXT NOT NULL
        )
        """,
    ]


def _get_pool():
    """延迟初始化连接池（首次调用时创建）"""
    global _pool
    if _pool is None:
        _pool = PooledDB(
            creator=_get_db_driver(),
            maxconnections=10,
            mincached=0,
            maxcached=5,
            blocking=True,
            **_get_pool_kwargs(),
        )
    return _pool


# ==================== 数据库相关 ====================


def get_db():
    """获取数据库连接（从连接池）"""
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = _get_pool().connection()
        _run_connection_init(db)
    return db


@app.teardown_appcontext
def close_connection(exception):
    """归还数据库连接到连接池"""
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


class _QueryResult:
    def __init__(self, rows=None, rowcount: int = 0):
        self._rows = list(rows or [])
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def _execute(db, sql, params=None):
    """执行 SQL 并返回兼容 fetchone/fetchall/rowcount 的结果对象"""
    cursor = db.cursor()
    try:
        cursor.execute(sql, params or ())
        rows = cursor.fetchall() if cursor.description else []
        return _QueryResult(rows=rows, rowcount=cursor.rowcount)
    finally:
        cursor.close()


def init_db():
    """初始化数据库表结构"""
    conn = _get_pool().connection()
    try:
        _run_connection_init(conn)
        cursor = conn.cursor()
        for sql in _get_schema_sqls():
            cursor.execute(sql)

        default_settings = [
            ("warranty_days", str(CONFIG["warranty"]["default_days"])),
            ("max_replacements", str(CONFIG["warranty"]["max_replacements"])),
            ("site_title", "Kiro 发卡平台"),
            ("site_notice", "欢迎使用Kiro发卡平台"),
        ]
        insert_sql = _sql_insert_default_setting()
        for key, value in default_settings:
            cursor.execute(insert_sql, (key, value))

        conn.commit()
    finally:
        conn.close()


def get_setting(key: str, default: str = "") -> str:
    """获取系统设置"""
    db = get_db()
    key_column = _quote_identifier("key")
    row = _execute(db, f"SELECT value FROM settings WHERE {key_column} = %s", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    """设置系统设置"""
    db = get_db()
    params = (key, value, value) if _is_mysql() else (key, value)
    _execute(db, _sql_upsert_setting(), params)
    db.commit()


# ==================== 账号校验相关 ====================


def verify_bearer_credential(
    authorization_header: str,
    idc_region: str = None,
    profile_arn: str = None,
) -> dict[str, Any]:
    """
    校验Bearer凭证是否有效（与kiro_full_flow_cn.py相同的方法）
    """
    if idc_region is None:
        idc_region = CONFIG["verification"]["idc_region"]
    if profile_arn is None:
        profile_arn = CONFIG["verification"]["profile_arn"]

    headers = {
        "Authorization": authorization_header,
        "accept": "application/json",
        "x-amz-user-agent": "aws-sdk-js/1.0.0 KiroIDE-script",
    }
    base = f"https://q.{idc_region}.amazonaws.com"
    timeout = CONFIG["verification"]["timeout_seconds"]

    out: dict[str, Any] = {
        "region": idc_region,
        "profile_arn": profile_arn,
        "get_usage_limits": {},
        "list_available_models": {},
    }

    # 1) getUsageLimits
    try:
        usage_url = f"{base}/getUsageLimits"
        usage_params = {
            "origin": "AI_EDITOR",
            "profileArn": profile_arn,
            "resourceType": "AGENTIC_REQUEST",
        }
        usage_resp = requests.get(
            usage_url, headers=headers, params=usage_params, timeout=timeout
        )
        usage_text = usage_resp.text
        usage_json = None
        try:
            usage_json = usage_resp.json()
        except Exception:
            pass
        out["get_usage_limits"] = {
            "status_code": usage_resp.status_code,
            "ok": usage_resp.ok,
            "response_json": usage_json,
            "response_text_preview": usage_text[:500] if usage_text else "",
        }
    except Exception as e:
        out["get_usage_limits"] = {
            "status_code": 0,
            "ok": False,
            "error": str(e),
        }

    # 2) ListAvailableModels
    try:
        models_url = f"{base}/ListAvailableModels"
        models_params = {
            "origin": "AI_EDITOR",
            "profileArn": profile_arn,
        }
        models_resp = requests.get(
            models_url, headers=headers, params=models_params, timeout=timeout
        )
        models_text = models_resp.text
        models_json = None
        try:
            models_json = models_resp.json()
        except Exception:
            pass
        model_count = None
        default_model_id = None
        if isinstance(models_json, dict):
            models = models_json.get("models")
            if isinstance(models, list):
                model_count = len(models)
            default_model = models_json.get("defaultModel")
            if isinstance(default_model, dict):
                default_model_id = default_model.get("modelId")
        out["list_available_models"] = {
            "status_code": models_resp.status_code,
            "ok": models_resp.ok,
            "model_count": model_count,
            "default_model_id": default_model_id,
            "response_json": models_json,
            "response_text_preview": models_text[:500] if models_text else "",
        }
    except Exception as e:
        out["list_available_models"] = {
            "status_code": 0,
            "ok": False,
            "error": str(e),
        }

    out["is_valid"] = bool(
        out["get_usage_limits"].get("ok") and out["list_available_models"].get("ok")
    )
    return out


def _parse_token_data(raw_token_data: Optional[str]) -> dict[str, Any]:
    if not raw_token_data:
        return {}
    try:
        parsed = json.loads(raw_token_data)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _pick_first_nonempty(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _extract_region_from_profile_arn(profile_arn: str) -> str:
    if not profile_arn:
        return ""
    parts = str(profile_arn).split(":")
    return parts[3].strip() if len(parts) >= 4 and parts[3].strip() else ""


def _generate_account_key(client_id: str, refresh_token: str, account_id: int) -> str:
    seed = _pick_first_nonempty(client_id, refresh_token, str(account_id), uuid4().hex)
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _build_kiro_runtime_headers(access_token: str, account_key: str, auth_method: str) -> dict[str, str]:
    normalized_auth_method = str(auth_method or "").strip().lower()

    # Align with CLIProxyAPIPlus v6.8.30-0 and earlier:
    # IDC accounts use KiroIDE-style dynamic fingerprint headers,
    # other auth types use the older static Amazon Q CLI-style headers.
    if normalized_auth_method != "idc":
        return {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Accept": "*/*",
            "x-amz-user-agent": "aws-sdk-rust/1.3.9 ua/2.1 api/ssooidc/1.88.0 os/macos lang/rust/1.87.0 m/E app/AmazonQ-For-CLI",
            "User-Agent": "aws-sdk-rust/1.3.9 os/macos lang/rust/1.87.0",
            "amz-sdk-invocation-id": str(uuid4()),
            "amz-sdk-request": "attempt=1; max=3",
        }

    token_hash = hashlib.sha256(account_key.encode("utf-8")).hexdigest()
    kiro_version = "0.8.1"
    sdk_version = "1.0.27"
    os_type = "win32"
    os_version = "10.0.19044"
    node_version = "22.21.1"

    return {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept": "*/*",
        "x-amz-user-agent": f"aws-sdk-js/{sdk_version} KiroIDE-{kiro_version}-{token_hash}",
        "User-Agent": (
            f"aws-sdk-js/{sdk_version} ua/2.1 os/{os_type}#{os_version} "
            f"lang/js md/nodejs#{node_version} api/codewhispererstreaming#{sdk_version} "
            f"m/E KiroIDE-{kiro_version}-{token_hash}"
        ),
        "amz-sdk-invocation-id": str(uuid4()),
        "amz-sdk-request": "attempt=1; max=3",
        "x-amzn-kiro-agent-mode": "vibe",
        "x-amzn-codewhisperer-optout": "true",
    }


def _build_kiro_warmup_payload(prompt: str, profile_arn: str) -> dict[str, Any]:
    return {
        "conversationState": {
            "agentTaskType": "vibe",
            "chatTriggerType": "MANUAL",
            "conversationId": str(uuid4()),
            "currentMessage": {
                "userInputMessage": {
                    "content": f"[Context: Current time is {beijing_now().strftime('%Y-%m-%d %H:%M:%S CST')}]\n\n{prompt}",
                    "modelId": "auto",
                    "origin": "AI_EDITOR",
                }
            },
        },
        "profileArn": profile_arn,
        "inferenceConfig": {
            "maxTokens": 32,
            "temperature": 0,
        },
    }


def resolve_account_verification_context(account: dict) -> dict[str, str]:
    """从账号字段和 token_data 中提取校验所需上下文。"""
    token_data = _parse_token_data(account["token_data"])
    token_output = token_data.get("token_output", {}) if isinstance(token_data, dict) else {}
    if not isinstance(token_output, dict):
        token_output = {}

    flow_data = token_data.get("flow", {}) if isinstance(token_data, dict) else {}
    if not isinstance(flow_data, dict):
        flow_data = {}

    region = _pick_first_nonempty(
        token_data.get("region"),
        token_data.get("idc_region"),
        token_data.get("idcRegion"),
        token_output.get("region"),
        flow_data.get("idcRegion"),
        CONFIG["verification"].get("idc_region"),
    )
    profile_arn = _pick_first_nonempty(
        token_data.get("profile_arn"),
        token_data.get("profileArn"),
        token_output.get("profile_arn"),
        token_output.get("profileArn"),
        CONFIG["verification"].get("profile_arn"),
    )
    client_id = _pick_first_nonempty(
        token_data.get("client_id"),
        token_data.get("clientId"),
        token_output.get("client_id"),
        token_output.get("clientId"),
    )
    client_secret = _pick_first_nonempty(
        token_data.get("client_secret"),
        token_data.get("clientSecret"),
        token_output.get("client_secret"),
        token_output.get("clientSecret"),
    )
    refresh_token = _pick_first_nonempty(
        account["refresh_token"],
        token_data.get("refresh_token"),
        token_data.get("refreshToken"),
        token_output.get("refresh_token"),
        token_output.get("refreshToken"),
    )
    access_token = _pick_first_nonempty(
        account["access_token"],
        token_data.get("access_token"),
        token_data.get("accessToken"),
        token_output.get("access_token"),
        token_output.get("accessToken"),
    )

    return {
        "region": region,
        "profile_arn": profile_arn,
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "access_token": access_token,
    }


def build_account_token_payload(acc: dict[str, Any]) -> dict[str, Any]:
    """统一生成 token_data，兼容直接导入 token JSON 的场景。"""
    raw_token_data = acc.get("token_data")
    if isinstance(raw_token_data, dict):
        return dict(raw_token_data)

    # 直接导入 token JSON 时，原样保存顶层结构
    direct_keys = {
        "access_token",
        "refresh_token",
        "id_token",
        "email",
        "profile_arn",
        "region",
        "client_id",
        "client_secret",
        "start_url",
        "provider",
        "type",
        "auth_method",
        "disabled",
        "expires_at",
        "last_refresh",
    }
    direct_payload = {
        k: acc[k] for k in acc.keys() if k in direct_keys and acc.get(k) is not None
    }
    if direct_payload:
        return direct_payload

    payload: dict[str, Any] = {}

    # 若导入的是 kiro-zhuce 的 result 文件结构，兼容 token_output
    token_output = acc.get("token_output")
    if isinstance(token_output, dict):
        for key in (
            "access_token",
            "refresh_token",
            "id_token",
            "client_id",
            "client_secret",
            "profile_arn",
            "region",
            "start_url",
            "auth_method",
            "provider",
            "type",
            "expires_at",
            "last_refresh",
            "disabled",
        ):
            if token_output.get(key) is not None:
                payload[key] = token_output.get(key)

    return payload


def refresh_access_token(
    refresh_token: str,
    client_id: str,
    client_secret: str,
    idc_region: str,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "ok": False,
        "status_code": 0,
        "response_json": None,
        "error": None,
    }
    if not refresh_token:
        out["error"] = "missing_refresh_token"
        return out
    if not client_id or not client_secret:
        out["error"] = "missing_client_credentials"
        return out
    if not idc_region:
        out["error"] = "missing_idc_region"
        return out

    url = f"https://oidc.{idc_region}.amazonaws.com/token"
    payload = {
        "clientId": client_id,
        "clientSecret": client_secret,
        "grantType": "refresh_token",
        "refreshToken": refresh_token,
    }
    timeout = CONFIG["verification"]["timeout_seconds"]
    try:
        resp = requests.post(url, json=payload, timeout=timeout)
        out["status_code"] = resp.status_code
        try:
            out["response_json"] = resp.json()
        except Exception:
            out["response_json"] = None
        if resp.ok and isinstance(out["response_json"], dict):
            out["ok"] = bool(out["response_json"].get("accessToken"))
            if not out["ok"]:
                out["error"] = "missing_access_token_in_refresh_response"
        elif not resp.ok:
            out["error"] = "refresh_http_error"
    except Exception as e:
        out["error"] = str(e)
    return out


_BLOCKED_PATTERN = re.compile(
    r"temporar(?:y|ily)\s+is\s+suspended|security\s+precaution|"
    r"locked\s+your\s+account|contact\s+our\s+support|temporarily\s+suspended",
    re.IGNORECASE,
)


def _extract_error_text(endpoint_result: dict[str, Any]) -> str:
    if not isinstance(endpoint_result, dict):
        return ""

    parts: list[str] = []
    resp_json = endpoint_result.get("response_json")
    if isinstance(resp_json, dict):
        for key in ("reason", "message", "error", "errorCode", "code", "__type"):
            value = resp_json.get(key)
            if value:
                parts.append(str(value))

    preview = endpoint_result.get("response_text_preview")
    if preview:
        parts.append(str(preview))

    err = endpoint_result.get("error")
    if err:
        parts.append(str(err))

    return "\n".join(parts)


def classify_credential_verification(detail: dict[str, Any]) -> dict[str, Any]:
    """
    将校验结果分类为 available / blocked / unknown。

    规则依据:
    - available: getUsageLimits 和 ListAvailableModels 都成功(HTTP 2xx)
    - blocked: 命中明确封禁证据(403 + TEMPORARILY_SUSPENDED 或 message 里有 suspended/locked)
    - unknown: 其余情况（网络错误、参数错误、配置错误、临时异常等）
    """
    usage = detail.get("get_usage_limits", {}) if isinstance(detail, dict) else {}
    models = detail.get("list_available_models", {}) if isinstance(detail, dict) else {}

    usage_ok = bool(usage.get("ok"))
    models_ok = bool(models.get("ok"))
    if usage_ok and models_ok:
        return {
            "classification": "available",
            "evidence": "usage_and_models_ok",
        }

    usage_code = int(usage.get("status_code") or 0)
    models_code = int(models.get("status_code") or 0)
    usage_text = _extract_error_text(usage)
    models_text = _extract_error_text(models)
    all_text = f"{usage_text}\n{models_text}"

    usage_reason = ""
    if isinstance(usage.get("response_json"), dict):
        usage_reason = str(usage["response_json"].get("reason") or "")
    models_reason = ""
    if isinstance(models.get("response_json"), dict):
        models_reason = str(models["response_json"].get("reason") or "")

    reason_hit = usage_reason == "TEMPORARILY_SUSPENDED" or models_reason == "TEMPORARILY_SUSPENDED"
    message_hit = bool(_BLOCKED_PATTERN.search(all_text))
    has_403 = usage_code == 403 or models_code == 403

    # 明确封禁: 403 且存在封禁文案/原因
    if has_403 and (reason_hit or message_hit):
        return {
            "classification": "blocked",
            "evidence": "temporarily_suspended",
        }

    return {
        "classification": "unknown",
        "evidence": "insufficient_evidence",
    }


def check_account_status(account_id: int, db=None) -> dict[str, Any]:
    """检查账号状态"""
    if db is None:
        db = get_db()
    account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()

    if not account:
        return {"valid": False, "error": "账号不存在", "status": "not_found"}

    ctx = resolve_account_verification_context(account)
    refresh_result = refresh_access_token(
        refresh_token=ctx["refresh_token"],
        client_id=ctx["client_id"],
        client_secret=ctx["client_secret"],
        idc_region=ctx["region"],
    )

    access_token = ctx["access_token"]
    refreshed_access_token = None
    refreshed_refresh_token = None
    refreshed_id_token = None

    if refresh_result["ok"] and isinstance(refresh_result.get("response_json"), dict):
        refreshed_access_token = _pick_first_nonempty(refresh_result["response_json"].get("accessToken"))
        refreshed_refresh_token = _pick_first_nonempty(refresh_result["response_json"].get("refreshToken"))
        refreshed_id_token = _pick_first_nonempty(refresh_result["response_json"].get("idToken"))
        if refreshed_access_token:
            access_token = refreshed_access_token

    if refresh_result.get("error") == "missing_client_credentials":
        _execute(db,
            "UPDATE accounts SET last_verified_at = %s WHERE id = %s",
            (to_db_datetime(utc_now()), account_id),
        )
        db.commit()
        return {
            "valid": False,
            "blocked": False,
            "classification": "unknown",
            "error": "缺少 client_id/client_secret，无法刷新 token",
            "status": "unknown",
            "evidence": "missing_client_credentials",
            "refresh": refresh_result,
            "verification_context": {
                "region": ctx["region"],
                "profile_arn": ctx["profile_arn"],
            },
        }

    if not access_token:
        return {
            "valid": False,
            "blocked": False,
            "classification": "unknown",
            "error": "无可用访问令牌",
            "status": "unknown",
            "evidence": "no_access_token_after_refresh",
            "refresh": refresh_result,
        }

    # 先刷新，再用账号自身 region/profileArn 校验
    result = verify_bearer_credential(
        f"Bearer {access_token}",
        idc_region=ctx["region"],
        profile_arn=ctx["profile_arn"],
    )
    classification = classify_credential_verification(result)
    new_status = classification["classification"]

    token_payload = _parse_token_data(account["token_data"])
    if refreshed_access_token and ("access_token" in token_payload):
        token_payload["access_token"] = refreshed_access_token
    if refreshed_refresh_token and ("refresh_token" in token_payload):
        token_payload["refresh_token"] = refreshed_refresh_token
    # 保持导入结构不变: 仅当原始数据已有 id_token 时才更新，不新增该字段。
    if refreshed_id_token and ("id_token" in token_payload):
        token_payload["id_token"] = refreshed_id_token
    token_data_json = json.dumps(token_payload, ensure_ascii=False) if token_payload else account["token_data"]

    # 仅在有明确结论时改写状态；unknown 保持原状态，避免误封/误放
    if new_status in {"available", "blocked"}:
        token_values = (
            refreshed_access_token or account["access_token"],
            refreshed_refresh_token or account["refresh_token"],
            refreshed_id_token or account["id_token"],
            new_status,
            to_db_datetime(utc_now()),
            account_id,
        )
        _execute(db,
            """UPDATE accounts
               SET access_token = %s, refresh_token = %s, id_token = %s, token_data = %s, status = %s, last_verified_at = %s
               WHERE id = %s""",
            (
                token_values[0],
                token_values[1],
                token_values[2],
                token_data_json,
                token_values[3],
                token_values[4],
                token_values[5],
            ),
        )
    else:
        token_values = (
            refreshed_access_token or account["access_token"],
            refreshed_refresh_token or account["refresh_token"],
            refreshed_id_token or account["id_token"],
            to_db_datetime(utc_now()),
            account_id,
        )
        _execute(db,
            """UPDATE accounts
               SET access_token = %s, refresh_token = %s, id_token = %s, token_data = %s, last_verified_at = %s
               WHERE id = %s""",
            (
                token_values[0],
                token_values[1],
                token_values[2],
                token_data_json,
                token_values[3],
                token_values[4],
            ),
        )
    db.commit()

    return {
        "valid": classification["classification"] == "available",
        "blocked": classification["classification"] == "blocked",
        "classification": classification["classification"],
        "evidence": classification["evidence"],
        "status": new_status,
        "refresh": refresh_result,
        "verification_context": {
            "region": ctx["region"],
            "profile_arn": ctx["profile_arn"],
        },
        "detail": result,
    }


def get_unordered_account(account_id: int, db) -> Optional[dict[str, Any]]:
    """获取未进入任何订单的账号。"""
    return _execute(
        db,
        """SELECT a.*
           FROM accounts a
           LEFT JOIN orders o ON o.account_id = a.id
           WHERE a.id = %s AND o.id IS NULL""",
        (account_id,),
    ).fetchone()


def query_account_quota(account_id: int, db=None) -> dict[str, Any]:
    """查询账号余额/额度信息"""
    if db is None:
        db = get_db()
    account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()

    if not account:
        return {"success": False, "error": "账号不存在", "status": "not_found"}

    ctx = resolve_account_verification_context(account)
    access_token = ctx["access_token"]
    refreshed_access_token = None
    refreshed_refresh_token = None
    refreshed_id_token = None
    refresh_result = None

    # 尝试刷新token
    if ctx["refresh_token"] and ctx["client_id"] and ctx["client_secret"] and ctx["region"]:
        refresh_result = refresh_access_token(
            refresh_token=ctx["refresh_token"],
            client_id=ctx["client_id"],
            client_secret=ctx["client_secret"],
            idc_region=ctx["region"],
        )
        if refresh_result.get("ok") and isinstance(refresh_result.get("response_json"), dict):
            refreshed_access_token = _pick_first_nonempty(refresh_result["response_json"].get("accessToken"))
            refreshed_refresh_token = _pick_first_nonempty(refresh_result["response_json"].get("refreshToken"))
            refreshed_id_token = _pick_first_nonempty(refresh_result["response_json"].get("idToken"))
            if refreshed_access_token:
                access_token = refreshed_access_token

    if not access_token:
        return {"success": False, "error": "无可用访问令牌", "status": "missing_access_token"}

    # 确定API region
    api_region = _pick_first_nonempty(
        _extract_region_from_profile_arn(ctx["profile_arn"]),
        CONFIG["verification"].get("idc_region"),
        "us-east-1",
    )

    # 调用 GetUsageLimits API
    try:
        headers = {
            "Content-Type": "application/x-amz-json-1.0",
            "x-amz-target": "AmazonCodeWhispererService.GetUsageLimits",
            "Authorization": f"Bearer {access_token}",
        }
        payload = {
            "origin": "AI_EDITOR",
            "resourceType": "AGENTIC_REQUEST",
        }
        url = f"https://codewhisperer.{api_region}.amazonaws.com"
        resp = requests.post(
            url,
            headers=headers,
            json=payload,
            timeout=CONFIG["verification"]["timeout_seconds"],
        )

        response_json = None
        try:
            response_json = resp.json()
        except Exception:
            pass

        # 更新token到数据库
        token_payload = _parse_token_data(account["token_data"])
        if refreshed_access_token and ("access_token" in token_payload):
            token_payload["access_token"] = refreshed_access_token
        if refreshed_refresh_token and ("refresh_token" in token_payload):
            token_payload["refresh_token"] = refreshed_refresh_token
        if refreshed_id_token and ("id_token" in token_payload):
            token_payload["id_token"] = refreshed_id_token
        token_data_json = json.dumps(token_payload, ensure_ascii=False) if token_payload else account["token_data"]

        _execute(
            db,
            """UPDATE accounts
               SET access_token = %s, refresh_token = %s, id_token = %s, token_data = %s, last_verified_at = %s
               WHERE id = %s""",
            (
                refreshed_access_token or account["access_token"],
                refreshed_refresh_token or account["refresh_token"],
                refreshed_id_token or account["id_token"],
                token_data_json,
                to_db_datetime(utc_now()),
                account_id,
            ),
        )
        db.commit()

        if not resp.ok:
            # 解析错误信息
            error_reason = ""
            error_message = ""
            if isinstance(response_json, dict):
                error_reason = response_json.get("reason", "")
                error_message = response_json.get("message", "")
            return {
                "success": False,
                "error": error_message or f"HTTP {resp.status_code}",
                "reason": error_reason,
                "status_code": resp.status_code,
                "response": response_json,
            }

        # 解析成功响应
        quota_info = {
            "success": True,
            "account_id": account_id,
            "email": account["email"],
            "status_code": resp.status_code,
        }

        if isinstance(response_json, dict):
            # 订阅信息
            sub_info = response_json.get("subscriptionInfo", {})
            quota_info["subscription_title"] = sub_info.get("subscriptionTitle", "Unknown")
            quota_info["subscription_type"] = sub_info.get("type", "")

            # 额度信息
            usage_list = response_json.get("usageBreakdownList", [])
            if usage_list:
                for usage in usage_list:
                    if usage.get("resourceType") == "CREDIT":
                        quota_info["credit_limit"] = usage.get("usageLimitWithPrecision", 0)
                        quota_info["credit_used"] = usage.get("currentUsageWithPrecision", 0)
                        quota_info["credit_remaining"] = quota_info["credit_limit"] - quota_info["credit_used"]

                        # 免费试用信息
                        trial_info = usage.get("freeTrialInfo")
                        if trial_info:
                            quota_info["trial_status"] = trial_info.get("freeTrialStatus", "")
                            quota_info["trial_limit"] = trial_info.get("usageLimitWithPrecision", 0)
                            quota_info["trial_used"] = trial_info.get("currentUsageWithPrecision", 0)
                            quota_info["trial_remaining"] = quota_info["trial_limit"] - quota_info["trial_used"]
                            trial_expiry = trial_info.get("freeTrialExpiry")
                            if trial_expiry:
                                quota_info["trial_expiry"] = datetime.fromtimestamp(trial_expiry, tz=UTC_TZ).strftime("%Y-%m-%d %H:%M:%S")

                        # 重置时间
                        next_reset = usage.get("nextDateReset") or response_json.get("nextDateReset")
                        if next_reset:
                            quota_info["next_reset"] = datetime.fromtimestamp(next_reset, tz=UTC_TZ).strftime("%Y-%m-%d %H:%M:%S")
                        break

            # 用户信息
            user_info = response_json.get("userInfo", {})
            quota_info["user_id"] = user_info.get("userId", "")

            # 原始响应（供调试）
            quota_info["raw_response"] = response_json

        return quota_info

    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "status": "request_failed",
        }


def warmup_account(account_id: int, db=None) -> dict[str, Any]:
    """对未出单账号执行一次简单对话预热。"""
    if db is None:
        db = get_db()

    account = get_unordered_account(account_id, db)
    if not account:
        return {"success": False, "error": "账号不存在或已在订单中", "status": "not_eligible"}

    if account["status"] == "blocked":
        return {"success": False, "error": "账号已封禁，不能预热", "status": "blocked"}

    ctx = resolve_account_verification_context(account)
    access_token = ctx["access_token"]
    refreshed_access_token = None
    refreshed_refresh_token = None
    refreshed_id_token = None
    refresh_result = None

    if ctx["refresh_token"] and ctx["client_id"] and ctx["client_secret"] and ctx["region"]:
        refresh_result = refresh_access_token(
            refresh_token=ctx["refresh_token"],
            client_id=ctx["client_id"],
            client_secret=ctx["client_secret"],
            idc_region=ctx["region"],
        )
        if refresh_result.get("ok") and isinstance(refresh_result.get("response_json"), dict):
            refreshed_access_token = _pick_first_nonempty(refresh_result["response_json"].get("accessToken"))
            refreshed_refresh_token = _pick_first_nonempty(refresh_result["response_json"].get("refreshToken"))
            refreshed_id_token = _pick_first_nonempty(refresh_result["response_json"].get("idToken"))
            if refreshed_access_token:
                access_token = refreshed_access_token

    if not access_token:
        return {"success": False, "error": "无可用访问令牌", "status": "missing_access_token", "refresh": refresh_result}

    api_region = _pick_first_nonempty(
        _extract_region_from_profile_arn(ctx["profile_arn"]),
        CONFIG["verification"].get("idc_region"),
        "us-east-1",
    )
    payload = _build_kiro_warmup_payload(CONFIG["warmup"]["prompt"], ctx["profile_arn"])
    headers = _build_kiro_runtime_headers(
        access_token,
        _generate_account_key(ctx["client_id"], ctx["refresh_token"], account_id),
        _pick_first_nonempty(
            _parse_token_data(account["token_data"]).get("auth_method"),
            _parse_token_data(account["token_data"]).get("authMethod"),
        ),
    )

    response_json = None
    response_text = ""
    try:
        resp = requests.post(
            f"https://q.{api_region}.amazonaws.com/generateAssistantResponse",
            headers=headers,
            json=payload,
            timeout=CONFIG["warmup"]["timeout_seconds"],
        )
        response_text = resp.text or ""
        try:
            response_json = resp.json()
        except Exception:
            response_json = None
    except Exception as e:
        return {
            "success": False,
            "status": "request_failed",
            "error": str(e),
            "refresh": refresh_result,
        }

    token_payload = _parse_token_data(account["token_data"])
    if refreshed_access_token and ("access_token" in token_payload):
        token_payload["access_token"] = refreshed_access_token
    if refreshed_refresh_token and ("refresh_token" in token_payload):
        token_payload["refresh_token"] = refreshed_refresh_token
    if refreshed_id_token and ("id_token" in token_payload):
        token_payload["id_token"] = refreshed_id_token
    token_data_json = json.dumps(token_payload, ensure_ascii=False) if token_payload else account["token_data"]

    _execute(
        db,
        """UPDATE accounts
           SET access_token = %s, refresh_token = %s, id_token = %s, token_data = %s, last_verified_at = %s
           WHERE id = %s""",
        (
            refreshed_access_token or account["access_token"],
            refreshed_refresh_token or account["refresh_token"],
            refreshed_id_token or account["id_token"],
            token_data_json,
            to_db_datetime(utc_now()),
            account_id,
        ),
    )
    db.commit()

    ok = resp.ok
    output_text = ""
    if isinstance(response_json, dict):
        output_text = _pick_first_nonempty(
            response_json.get("content"),
            response_json.get("message"),
            response_json.get("assistantResponseMessage", {}).get("content") if isinstance(response_json.get("assistantResponseMessage"), dict) else "",
        )

    return {
        "success": ok,
        "status": "ok" if ok else "upstream_error",
        "account_id": account_id,
        "email": account["email"],
        "http_status": resp.status_code,
        "response": response_json,
        "response_preview": output_text or response_text[:500],
        "refresh": refresh_result,
        "api_region": api_region,
    }


# ==================== CDK相关 ====================


def generate_cdk(count: int = 1) -> list[str]:
    """生成CDK码"""
    prefix = CONFIG["cdk"]["prefix"]
    length = CONFIG["cdk"]["length"]
    codes = []

    for _ in range(count):
        random_part = "".join(
            random.choices(string.ascii_uppercase + string.digits, k=length - len(prefix) - 1)
        )
        code = f"{prefix}-{random_part}"
        codes.append(code)

    return codes


def create_cdks(count: int, bind_account_ids: list[int] = None) -> list[dict]:
    """创建CDK"""
    db = get_db()
    codes = generate_cdk(count)
    results = []

    for i, code in enumerate(codes):
        account_id = bind_account_ids[i] if bind_account_ids and i < len(bind_account_ids) else None
        try:
            _execute(db,
                "INSERT INTO cdks (code, account_id, status) VALUES (%s, %s, %s)",
                (code, account_id, "unused"),
            )
            results.append({"code": code, "success": True})
        except _INTEGRITY_ERRORS:
            results.append({"code": code, "success": False, "error": "重复的CDK"})

    db.commit()
    return results


# ==================== 认证装饰器 ====================


def admin_required(f):
    """管理员认证装饰器"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get("admin_logged_in"):
            if request.is_json:
                return jsonify({"error": "未授权"}), 401
            return redirect(url_for("admin_login"))
        return f(*args, **kwargs)
    return decorated_function


# ==================== 用户路由 ====================


@app.route("/")
def index():
    """用户首页"""
    site_title = get_setting("site_title", "Kiro 发卡平台")
    site_notice = get_setting("site_notice", "")
    return render_template("index.html", site_title=site_title, site_notice=site_notice)


@app.route("/api/redeem", methods=["POST"])
def redeem_cdk():
    """使用CDK提卡"""
    data = request.get_json()
    cdk_code = data.get("cdk", "").strip().upper()

    if not cdk_code:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    db = get_db()
    cdk = _execute(db, "SELECT * FROM cdks WHERE code = %s", (cdk_code,)).fetchone()

    if not cdk:
        return jsonify({"success": False, "error": "CDK不存在"}), 404

    if cdk["status"] == "used":
        # 检查是否有订单
        order = _execute(db,
            "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)
        ).fetchone()
        if order:
            # 返回订单信息让用户可以查看
            return jsonify({
                "success": True,
                "already_used": True,
                "order_id": order["id"],
                "message": "此CDK已使用，可查看订单详情"
            })
        return jsonify({"success": False, "error": "CDK已被使用"}), 400

    # 获取绑定的账号或分配一个可用账号
    account_id = cdk["account_id"]
    bound_account = account_id is not None

    if not account_id:
        # 动态分配账号：尝试最多10个账号进行校验，找到可用的
        tried_account_ids = []
        max_attempts = 10
        found_available = False

        for attempt in range(max_attempts):
            exclude_clause = ""
            params = []
            if tried_account_ids:
                placeholders = ",".join(["%s"] * len(tried_account_ids))
                exclude_clause = f"AND id NOT IN ({placeholders})"
                params = tried_account_ids.copy()

            available_account = _execute(db,
                f"""SELECT * FROM accounts WHERE status = 'available' {exclude_clause}
                    AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
                    LIMIT 1""",
                params
            ).fetchone()

            if not available_account:
                break

            trial_account_id = available_account["id"]
            tried_account_ids.append(trial_account_id)

            # 校验账号
            verify_result = check_account_status(trial_account_id, db)
            verify_classification = verify_result.get("classification", "unknown")
            verify_details = json.dumps({
                "error": verify_result.get("error"),
                "evidence": verify_result.get("evidence"),
            }, ensure_ascii=False)

            # 记录尝试校验结果
            _execute(db,
                """INSERT INTO verifications (account_id, verification_type, result, details, verified_at)
                   VALUES (%s, 'redeem_trial', %s, %s, %s)""",
                (trial_account_id, verify_classification, verify_details, to_db_datetime(utc_now()))
            )

            if verify_classification == "available":
                account_id = trial_account_id
                final_verify_classification = verify_classification
                final_verify_details = verify_details
                found_available = True
                break
            elif verify_classification == "blocked":
                _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (trial_account_id,))

        if not found_available or not account_id:
            db.commit()
            return jsonify({"success": False, "error": "暂无可用账号，请稍后重试"}), 400

        # 获取最终分配的账号信息
        account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()

    else:
        # CDK绑定了账号，校验该账号
        account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()
        if not account:
            return jsonify({"success": False, "error": "绑定的账号不存在"}), 400

        verify_result = check_account_status(account_id, db)
        final_verify_classification = verify_result.get("classification", "unknown")
        final_verify_details = json.dumps({
            "error": verify_result.get("error"),
            "evidence": verify_result.get("evidence"),
        }, ensure_ascii=False)

        _execute(db,
            """INSERT INTO verifications (account_id, verification_type, result, details, verified_at)
               VALUES (%s, 'redeem_bound', %s, %s, %s)""",
            (account_id, final_verify_classification, final_verify_details, to_db_datetime(utc_now()))
        )

        if final_verify_classification == "blocked":
            _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (account_id,))
            db.commit()
            return jsonify({"success": False, "error": "绑定的账号已被封禁，请联系客服更换"}), 400

        if final_verify_classification == "unknown":
            db.commit()
            return jsonify({"success": False, "error": "账号状态不明确，请稍后重试"}), 400

    # 获取质保天数
    warranty_days = int(get_setting("warranty_days", "7"))
    warranty_expires = utc_now() + timedelta(days=warranty_days)

    # 创建订单
    _execute(db,
        """INSERT INTO orders
           (cdk_code, account_id, user_ip, warranty_days, warranty_expires_at, status)
           VALUES (%s, %s, %s, %s, %s, 'active')""",
        (cdk_code, account_id, request.remote_addr, warranty_days, to_db_datetime(warranty_expires)),
    )

    # 更新CDK状态
    _execute(db,
        "UPDATE cdks SET status = 'used', used_at = %s, used_by = %s, account_id = %s WHERE code = %s",
        (to_db_datetime(utc_now()), request.remote_addr, account_id, cdk_code),
    )

    # 获取订单ID
    order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

    # 记录提卡校验结果
    _execute(db,
        """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
           VALUES (%s, %s, 'redeem', %s, %s, %s)""",
        (account_id, order["id"], final_verify_classification, final_verify_details, to_db_datetime(utc_now()))
    )

    db.commit()

    # 解析token数据
    token_data = {}
    if account["token_data"]:
        try:
            token_data = json.loads(account["token_data"])
        except:
            pass

    return jsonify({
        "success": True,
        "order_id": order["id"],
        "account": {
            "email": account["email"],
            "access_token": account["access_token"],
            "refresh_token": account["refresh_token"],
            "id_token": account["id_token"],
            "token_data": token_data,
        },
        "warranty": {
            "days": warranty_days,
            "expires_at": warranty_expires.isoformat(),
        }
    })


@app.route("/api/order/<int:order_id>")
def get_order(order_id):
    """获取订单信息"""
    db = get_db()
    order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = _execute(db,
        "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)
    ).fetchone()

    token_data = {}
    if account and account["token_data"]:
        try:
            token_data = json.loads(account["token_data"])
        except:
            pass

    return jsonify({
        "success": True,
        "order": {
            "id": order["id"],
            "cdk_code": order["cdk_code"],
            "status": order["status"],
            "warranty_days": order["warranty_days"],
            "warranty_expires_at": to_api_datetime(order["warranty_expires_at"]),
            "replacement_count": order["replacement_count"],
            "created_at": to_api_datetime(order["created_at"]),
        },
        "account": {
            "email": account["email"] if account else None,
            "access_token": account["access_token"] if account else None,
            "refresh_token": account["refresh_token"] if account else None,
            "id_token": account["id_token"] if account else None,
            "token_data": token_data,
            "status": account["status"] if account else None,
        } if account else None
    })


@app.route("/api/order/<int:order_id>/download")
def download_order_json(order_id):
    """下载订单账号JSON，导出格式适配CPA。"""
    db = get_db()
    order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()
    if not account:
        return jsonify({"success": False, "error": "账号不存在"}), 404

    token_data = _parse_token_data(account["token_data"])

    # 以原始导入结构为准
    if token_data:
        payload = dict(token_data)
    else:
        payload = {}

    # 从数据库补充核心字段
    if account["access_token"]:
        payload["access_token"] = account["access_token"]
    if account["refresh_token"]:
        payload["refresh_token"] = account["refresh_token"]
    if account["id_token"]:
        payload["id_token"] = account["id_token"]
    if account["email"]:
        payload["email"] = account["email"]

    # CPA固定字段（如果原数据没有则补充默认值）
    if "auth_method" not in payload:
        payload["auth_method"] = "builder-id"
    if "disabled" not in payload:
        payload["disabled"] = False
    if "provider" not in payload:
        payload["provider"] = "AWS"
    if "start_url" not in payload:
        payload["start_url"] = "https://view.awsapps.com/start"
    if "type" not in payload:
        payload["type"] = "kiro"

    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    email_for_name = re.sub(r"[^A-Za-z0-9@._-]+", "_", str(account["email"] or "unknown").strip())
    filename = f"{email_for_name}_{order_id}.json"
    return send_file(
        io.BytesIO(content),
        as_attachment=True,
        download_name=filename,
        mimetype="application/json",
    )


@app.route("/api/order/<int:order_id>/download/kiro")
def download_order_kiro(order_id):
    """下载订单账号JSON，导出格式适配Kiro Account Manager。"""
    db = get_db()
    order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()
    if not account:
        return jsonify({"success": False, "error": "账号不存在"}), 404

    token_data = _parse_token_data(account["token_data"])

    # 从 token_data 获取 client_id 和 client_secret
    client_id = ""
    client_secret = ""
    if token_data:
        client_id = token_data.get("client_id") or token_data.get("clientId") or ""
        client_secret = token_data.get("client_secret") or token_data.get("clientSecret") or ""

    # Kiro Account Manager 格式
    payload = {
        "refreshToken": account["refresh_token"] or "",
        "clientId": client_id,
        "clientSecret": client_secret,
        "provider": "BuilderId"
    }

    content = json.dumps([payload], ensure_ascii=False, indent=2).encode("utf-8")
    email_for_name = re.sub(r"[^A-Za-z0-9@._-]+", "_", str(account["email"] or "unknown").strip())
    filename = f"{email_for_name}_{order_id}_kiro.json"
    return send_file(
        io.BytesIO(content),
        as_attachment=True,
        download_name=filename,
        mimetype="application/json",
    )


@app.route("/api/orders/download", methods=["POST"])
def download_orders_batch():
    """批量下载订单账号JSON（CPA格式，单文件合并为数组）"""
    data = request.get_json()
    order_ids = data.get("order_ids", [])

    if not order_ids:
        return jsonify({"success": False, "error": "请提供订单ID"}), 400

    if len(order_ids) > 100:
        return jsonify({"success": False, "error": "单次最多下载100个订单"}), 400

    db = get_db()
    payloads = []

    for order_id in order_ids:
        order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
        if not order:
            continue

        account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()
        if not account:
            continue

        token_data = _parse_token_data(account["token_data"])

        if token_data:
            payload = dict(token_data)
        else:
            payload = {}

        if account["access_token"]:
            payload["access_token"] = account["access_token"]
        if account["refresh_token"]:
            payload["refresh_token"] = account["refresh_token"]
        if account["id_token"]:
            payload["id_token"] = account["id_token"]
        if account["email"]:
            payload["email"] = account["email"]

        if "auth_method" not in payload:
            payload["auth_method"] = "builder-id"
        if "disabled" not in payload:
            payload["disabled"] = False
        if "provider" not in payload:
            payload["provider"] = "AWS"
        if "start_url" not in payload:
            payload["start_url"] = "https://view.awsapps.com/start"
        if "type" not in payload:
            payload["type"] = "kiro"

        payloads.append(payload)

    if not payloads:
        return jsonify({"success": False, "error": "没有有效的订单"}), 400

    content = json.dumps(payloads, ensure_ascii=False, indent=2).encode("utf-8")
    filename = f"accounts_batch_{len(payloads)}_{beijing_now().strftime('%Y%m%d_%H%M%S')}.json"
    return send_file(
        io.BytesIO(content),
        as_attachment=True,
        download_name=filename,
        mimetype="application/json",
    )


@app.route("/api/orders/download/kiro", methods=["POST"])
def download_orders_batch_kiro():
    """批量下载订单账号JSON（Kiro Account Manager格式，单文件合并为数组）"""
    data = request.get_json()
    order_ids = data.get("order_ids", [])

    if not order_ids:
        return jsonify({"success": False, "error": "请提供订单ID"}), 400

    if len(order_ids) > 100:
        return jsonify({"success": False, "error": "单次最多下载100个订单"}), 400

    db = get_db()
    payloads = []

    for order_id in order_ids:
        order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
        if not order:
            continue

        account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()
        if not account:
            continue

        token_data = _parse_token_data(account["token_data"])

        client_id = ""
        client_secret = ""
        if token_data:
            client_id = token_data.get("client_id") or token_data.get("clientId") or ""
            client_secret = token_data.get("client_secret") or token_data.get("clientSecret") or ""

        payload = {
            "refreshToken": account["refresh_token"] or "",
            "clientId": client_id,
            "clientSecret": client_secret,
            "provider": "BuilderId"
        }
        payloads.append(payload)

    if not payloads:
        return jsonify({"success": False, "error": "没有有效的订单"}), 400

    content = json.dumps(payloads, ensure_ascii=False, indent=2).encode("utf-8")
    filename = f"accounts_kiro_batch_{len(payloads)}_{beijing_now().strftime('%Y%m%d_%H%M%S')}.json"
    return send_file(
        io.BytesIO(content),
        as_attachment=True,
        download_name=filename,
        mimetype="application/json",
    )


@app.route("/api/warranty/check", methods=["POST"])
def check_warranty():
    """检查质保状态"""
    data = request.get_json()
    cdk_code = data.get("cdk", "").strip().upper()

    if not cdk_code:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    db = get_db()
    order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = _execute(db,
        "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)
    ).fetchone()

    # 检查账号状态
    account_check = check_account_status(order["account_id"]) if account else None

    # 检查是否在质保期内
    warranty_expires = to_utc_time(order["warranty_expires_at"])
    in_warranty = utc_now() < warranty_expires

    # 检查是否可以替换
    max_replacements = int(get_setting("max_replacements", "3"))
    can_replace = (
        in_warranty
        and order["replacement_count"] < max_replacements
        and account_check
        and account_check.get("classification") == "blocked"
    )

    return jsonify({
        "success": True,
        "order": {
            "id": order["id"],
            "cdk_code": order["cdk_code"],
            "status": order["status"],
            "warranty_expires_at": to_api_datetime(order["warranty_expires_at"]),
            "replacement_count": order["replacement_count"],
            "max_replacements": max_replacements,
        },
        "account": {
            "email": account["email"] if account else None,
            "status": account["status"] if account else None,
            "is_valid": account_check["valid"] if account_check else False,
            "classification": account_check.get("classification") if account_check else "unknown",
        },
        "warranty": {
            "in_warranty": in_warranty,
            "can_replace": can_replace,
            "reason": (
                "账号正常" if account_check and account_check["valid"]
                else "校验结果不明确，暂不可替换" if account_check and account_check.get("classification") == "unknown"
                else "已超出最大替换次数" if order["replacement_count"] >= max_replacements
                else "质保已过期" if not in_warranty
                else "可以申请替换"
            )
        }
    })


@app.route("/api/warranty/replace", methods=["POST"])
def request_replacement():
    """申请质保替换"""
    data = request.get_json()
    cdk_code = data.get("cdk", "").strip().upper()

    if not cdk_code:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    db = get_db()
    order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    # 检查质保期
    warranty_expires = to_utc_time(order["warranty_expires_at"])
    if utc_now() >= warranty_expires:
        return jsonify({"success": False, "error": "质保已过期"}), 400

    # 检查替换次数
    max_replacements = int(get_setting("max_replacements", "3"))
    if order["replacement_count"] >= max_replacements:
        return jsonify({"success": False, "error": "已超出最大替换次数"}), 400

    # 检查账号是否真的被封禁
    account_check = check_account_status(order["account_id"], db)
    old_verify_classification = account_check.get("classification", "unknown")
    old_verify_details = json.dumps({
        "error": account_check.get("error"),
        "evidence": account_check.get("evidence"),
    }, ensure_ascii=False)

    # 记录旧账号校验结果
    _execute(db,
        """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
           VALUES (%s, %s, 'replace_old', %s, %s, %s)""",
        (order["account_id"], order["id"], old_verify_classification, old_verify_details, to_db_datetime(utc_now()))
    )

    if old_verify_classification == "available":
        db.commit()
        return jsonify({"success": False, "error": "账号状态正常，无需替换"}), 400
    if old_verify_classification != "blocked":
        db.commit()
        return jsonify({"success": False, "error": "当前无法明确判断账号已封禁，请稍后重试"}), 400

    old_account_id = order["account_id"]

    # 查找新的可用账号（排除所有已有订单的账号，防止重复分配）
    new_account = _execute(db,
        """SELECT * FROM accounts WHERE status = 'available'
           AND id != %s AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
           LIMIT 1""",
        (old_account_id,)
    ).fetchone()

    if not new_account:
        db.commit()
        return jsonify({"success": False, "error": "暂无可用账号进行替换"}), 400

    new_account_id = new_account["id"]

    # 校验新账号
    new_account_check = check_account_status(new_account_id, db)
    new_verify_classification = new_account_check.get("classification", "unknown")
    new_verify_details = json.dumps({
        "error": new_account_check.get("error"),
        "evidence": new_account_check.get("evidence"),
    }, ensure_ascii=False)

    # 更新订单
    _execute(db,
        """UPDATE orders SET account_id = %s, replacement_count = replacement_count + 1
           WHERE id = %s""",
        (new_account_id, order["id"])
    )

    # 记录替换历史
    _execute(db,
        """INSERT INTO replacements (order_id, old_account_id, new_account_id, reason)
           VALUES (%s, %s, %s, %s)""",
        (order["id"], old_account_id, new_account_id,
         f"旧账号校验: {old_verify_classification}, 新账号校验: {new_verify_classification}")
    )

    # 记录新账号校验结果
    _execute(db,
        """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
           VALUES (%s, %s, 'replace_new', %s, %s, %s)""",
        (new_account_id, order["id"], new_verify_classification, new_verify_details, to_db_datetime(utc_now()))
    )

    # 标记旧账号
    _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (old_account_id,))

    db.commit()

    # 解析token数据
    token_data = {}
    if new_account["token_data"]:
        try:
            token_data = json.loads(new_account["token_data"])
        except:
            pass

    return jsonify({
        "success": True,
        "message": "替换成功",
        "order_id": order["id"],
        "new_account": {
            "email": new_account["email"],
            "access_token": new_account["access_token"],
            "refresh_token": new_account["refresh_token"],
            "id_token": new_account["id_token"],
            "token_data": token_data,
        },
        "replacement_count": order["replacement_count"] + 1,
        "max_replacements": max_replacements,
    })


# ==================== 批量接口 ====================


@app.route("/api/redeem/batch", methods=["POST"])
def redeem_cdk_batch():
    """批量CDK提卡"""
    data = request.get_json()
    cdks_input = data.get("cdks", "").strip()

    if not cdks_input:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    # 解析CDK列表（支持换行、逗号、空格分隔）
    cdks = []
    for line in cdks_input.replace(",", "\n").replace(";", "\n").split("\n"):
        cdk = line.strip().upper()
        if cdk:
            cdks.append(cdk)

    # 去重
    cdks = list(dict.fromkeys(cdks))

    if not cdks:
        return jsonify({"success": False, "error": "未找到有效的CDK"}), 400

    if len(cdks) > 100:
        return jsonify({"success": False, "error": "单次最多处理100个CDK"}), 400

    db = get_db()
    warranty_days = int(get_setting("warranty_days", "7"))
    results = []
    success_count = 0
    failed_count = 0

    for cdk_code in cdks:
        try:
            cdk = _execute(db, "SELECT * FROM cdks WHERE code = %s", (cdk_code,)).fetchone()

            if not cdk:
                results.append({"cdk": cdk_code, "success": False, "error": "CDK不存在"})
                failed_count += 1
                continue

            if cdk["status"] == "used":
                # 检查是否有订单
                order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()
                if order:
                    acc = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()
                    token_data = {}
                    if acc and acc["token_data"]:
                        try:
                            token_data = json.loads(acc["token_data"])
                        except:
                            pass
                    results.append({
                        "cdk": cdk_code,
                        "success": True,
                        "already_used": True,
                        "order_id": order["id"],
                        "account": {
                            "email": acc["email"] if acc else None,
                            "access_token": acc["access_token"] if acc else None,
                            "refresh_token": acc["refresh_token"] if acc else None,
                            "id_token": acc["id_token"] if acc else None,
                            "token_data": token_data,
                        } if acc else None,
                        "message": "CDK已使用，返回已有订单"
                    })
                    success_count += 1
                else:
                    results.append({"cdk": cdk_code, "success": False, "error": "CDK已被使用但无订单记录"})
                    failed_count += 1
                continue

            # 获取绑定的账号或分配一个可用账号（带校验）
            account_id = cdk["account_id"]
            bound_account = account_id is not None
            account = None
            final_verify_classification = None
            final_verify_details = None

            if not account_id:
                # 动态分配账号：尝试最多10个账号进行校验，找到可用的
                tried_account_ids = []
                max_attempts = 10
                found_available = False

                for attempt in range(max_attempts):
                    exclude_clause = ""
                    params = []
                    if tried_account_ids:
                        placeholders = ",".join(["%s"] * len(tried_account_ids))
                        exclude_clause = f"AND id NOT IN ({placeholders})"
                        params = tried_account_ids.copy()

                    available_account = _execute(db,
                        f"""SELECT * FROM accounts WHERE status = 'available' {exclude_clause}
                            AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
                            LIMIT 1""",
                        params
                    ).fetchone()

                    if not available_account:
                        break

                    trial_account_id = available_account["id"]
                    tried_account_ids.append(trial_account_id)

                    # 校验账号
                    verify_result = check_account_status(trial_account_id, db)
                    verify_classification = verify_result.get("classification", "unknown")
                    verify_details = json.dumps({
                        "error": verify_result.get("error"),
                        "evidence": verify_result.get("evidence"),
                    }, ensure_ascii=False)

                    # 记录尝试校验结果
                    _execute(db,
                        """INSERT INTO verifications (account_id, verification_type, result, details, verified_at)
                           VALUES (%s, 'redeem_trial', %s, %s, %s)""",
                        (trial_account_id, verify_classification, verify_details, to_db_datetime(utc_now()))
                    )

                    if verify_classification == "available":
                        account_id = trial_account_id
                        account = available_account
                        final_verify_classification = verify_classification
                        final_verify_details = verify_details
                        found_available = True
                        break
                    elif verify_classification == "blocked":
                        _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (trial_account_id,))

                if not found_available or not account_id:
                    results.append({"cdk": cdk_code, "success": False, "error": "暂无可用账号，请稍后重试"})
                    failed_count += 1
                    continue

            else:
                # CDK绑定了账号，校验该账号
                account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()
                if not account:
                    results.append({"cdk": cdk_code, "success": False, "error": "绑定的账号不存在"})
                    failed_count += 1
                    continue

                verify_result = check_account_status(account_id, db)
                final_verify_classification = verify_result.get("classification", "unknown")
                final_verify_details = json.dumps({
                    "error": verify_result.get("error"),
                    "evidence": verify_result.get("evidence"),
                }, ensure_ascii=False)

                _execute(db,
                    """INSERT INTO verifications (account_id, verification_type, result, details, verified_at)
                       VALUES (%s, 'redeem_bound', %s, %s, %s)""",
                    (account_id, final_verify_classification, final_verify_details, to_db_datetime(utc_now()))
                )

                if final_verify_classification == "blocked":
                    _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (account_id,))
                    results.append({"cdk": cdk_code, "success": False, "error": "绑定的账号已被封禁，请联系客服更换"})
                    failed_count += 1
                    continue

                if final_verify_classification == "unknown":
                    results.append({"cdk": cdk_code, "success": False, "error": "账号状态不明确，请稍后重试"})
                    failed_count += 1
                    continue

            # 创建订单
            warranty_expires = utc_now() + timedelta(days=warranty_days)
            _execute(db,
                """INSERT INTO orders
                   (cdk_code, account_id, user_ip, warranty_days, warranty_expires_at, status)
                   VALUES (%s, %s, %s, %s, %s, 'active')""",
                (cdk_code, account_id, request.remote_addr, warranty_days, to_db_datetime(warranty_expires)),
            )

            # 更新CDK状态
            _execute(db,
                "UPDATE cdks SET status = 'used', used_at = %s, used_by = %s, account_id = %s WHERE code = %s",
                (to_db_datetime(utc_now()), request.remote_addr, account_id, cdk_code),
            )

            # 获取订单ID
            order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

            # 记录提卡校验结果
            _execute(db,
                """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
                   VALUES (%s, %s, 'redeem', %s, %s, %s)""",
                (account_id, order["id"], final_verify_classification, final_verify_details, to_db_datetime(utc_now()))
            )

            # 解析token数据
            token_data = {}
            if account["token_data"]:
                try:
                    token_data = json.loads(account["token_data"])
                except:
                    pass

            results.append({
                "cdk": cdk_code,
                "success": True,
                "order_id": order["id"],
                "account": {
                    "email": account["email"],
                    "access_token": account["access_token"],
                    "refresh_token": account["refresh_token"],
                    "id_token": account["id_token"],
                    "token_data": token_data,
                },
                "warranty": {
                    "days": warranty_days,
                    "expires_at": warranty_expires.isoformat(),
                }
            })
            success_count += 1

        except Exception as e:
            results.append({"cdk": cdk_code, "success": False, "error": str(e)})
            failed_count += 1

    db.commit()

    return jsonify({
        "success": True,
        "total": len(cdks),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    })


@app.route("/api/warranty/check-batch", methods=["POST"])
def check_warranty_batch():
    """批量质保查询 - 调用真实检验接口检查账号状态"""
    data = request.get_json()
    cdks_input = data.get("cdks", "").strip()

    if not cdks_input:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    # 解析CDK列表
    cdks = []
    for line in cdks_input.replace(",", "\n").replace(";", "\n").split("\n"):
        cdk = line.strip().upper()
        if cdk:
            cdks.append(cdk)

    cdks = list(dict.fromkeys(cdks))

    if not cdks:
        return jsonify({"success": False, "error": "未找到有效的CDK"}), 400

    if len(cdks) > 100:
        return jsonify({"success": False, "error": "单次最多查询100个CDK"}), 400

    db = get_db()
    max_replacements = int(get_setting("max_replacements", "3"))
    results = []

    for cdk_code in cdks:
        try:
            order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

            if not order:
                results.append({"cdk": cdk_code, "success": False, "error": "订单不存在"})
                continue

            account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (order["account_id"],)).fetchone()

            # 调用真实检验接口检查账号状态
            account_check = check_account_status(order["account_id"]) if account else None

            # 检查是否在质保期内
            warranty_expires = to_utc_time(order["warranty_expires_at"])
            in_warranty = utc_now() < warranty_expires

            # 判断是否可以替换：质保内 + 未超替换次数 + 账号被封禁
            can_replace = (
                in_warranty
                and order["replacement_count"] < max_replacements
                and account_check
                and account_check.get("classification") == "blocked"
            )

            # 更新数据库中的账号状态
            if account_check and account:
                new_status = account_check.get("classification", account["status"])
                if new_status != account["status"]:
                    _execute(db, "UPDATE accounts SET status = %s WHERE id = %s", (new_status, account["id"]))

            results.append({
                "cdk": cdk_code,
                "success": True,
                "order": {
                    "id": order["id"],
                    "cdk_code": order["cdk_code"],
                    "status": order["status"],
                    "warranty_days": order["warranty_days"],
                    "warranty_expires_at": to_api_datetime(order["warranty_expires_at"]),
                    "replacement_count": order["replacement_count"],
                },
                "account": {
                    "email": account["email"] if account else None,
                    "status": account["status"] if account else None,
                    "is_valid": account_check["valid"] if account_check else False,
                    "classification": account_check.get("classification") if account_check else "unknown",
                } if account else None,
                "warranty": {
                    "in_warranty": in_warranty,
                    "can_replace": can_replace,
                    "max_replacements": max_replacements,
                }
            })

        except Exception as e:
            results.append({"cdk": cdk_code, "success": False, "error": str(e)})

    db.commit()

    return jsonify({
        "success": True,
        "total": len(cdks),
        "results": results,
    })


@app.route("/api/warranty/replace-batch", methods=["POST"])
def request_replacement_batch():
    """批量质保替换"""
    data = request.get_json()
    cdks_input = data.get("cdks", "").strip()

    if not cdks_input:
        return jsonify({"success": False, "error": "请输入CDK"}), 400

    # 解析CDK列表
    cdks = []
    for line in cdks_input.replace(",", "\n").replace(";", "\n").split("\n"):
        cdk = line.strip().upper()
        if cdk:
            cdks.append(cdk)

    cdks = list(dict.fromkeys(cdks))

    if not cdks:
        return jsonify({"success": False, "error": "未找到有效的CDK"}), 400

    if len(cdks) > 50:
        return jsonify({"success": False, "error": "单次最多替换50个CDK"}), 400

    db = get_db()
    max_replacements = int(get_setting("max_replacements", "3"))
    results = []
    success_count = 0
    failed_count = 0

    for cdk_code in cdks:
        try:
            order = _execute(db, "SELECT * FROM orders WHERE cdk_code = %s", (cdk_code,)).fetchone()

            if not order:
                results.append({"cdk": cdk_code, "success": False, "error": "订单不存在"})
                failed_count += 1
                continue

            # 检查质保期
            warranty_expires = to_utc_time(order["warranty_expires_at"])
            if utc_now() >= warranty_expires:
                results.append({"cdk": cdk_code, "success": False, "error": "质保已过期"})
                failed_count += 1
                continue

            # 检查替换次数
            if order["replacement_count"] >= max_replacements:
                results.append({"cdk": cdk_code, "success": False, "error": "已超出最大替换次数"})
                failed_count += 1
                continue

            # 检查账号是否被封禁（进行校验）
            old_account_check = check_account_status(order["account_id"], db)
            old_verify_classification = old_account_check.get("classification", "unknown")
            old_verify_details = json.dumps({
                "error": old_account_check.get("error"),
                "evidence": old_account_check.get("evidence"),
            }, ensure_ascii=False)

            # 记录旧账号校验结果
            _execute(db,
                """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
                   VALUES (%s, %s, 'replace_old', %s, %s, %s)""",
                (order["account_id"], order["id"], old_verify_classification, old_verify_details, to_db_datetime(utc_now()))
            )

            if old_verify_classification != "blocked":
                results.append({"cdk": cdk_code, "success": False, "error": f"账号校验结果为{old_verify_classification}，不可替换"})
                failed_count += 1
                continue

            old_account_id = order["account_id"]

            # 查找新的可用账号
            new_account = _execute(db,
                """SELECT * FROM accounts WHERE status = 'available'
                   AND id != %s AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)
                   LIMIT 1""",
                (old_account_id,)
            ).fetchone()

            if not new_account:
                results.append({"cdk": cdk_code, "success": False, "error": "暂无可用账号"})
                failed_count += 1
                continue

            new_account_id = new_account["id"]

            # 校验新账号
            new_account_check = check_account_status(new_account_id, db)
            new_verify_classification = new_account_check.get("classification", "unknown")
            new_verify_details = json.dumps({
                "error": new_account_check.get("error"),
                "evidence": new_account_check.get("evidence"),
            }, ensure_ascii=False)

            # 更新订单
            _execute(db,
                """UPDATE orders SET account_id = %s, replacement_count = replacement_count + 1
                   WHERE id = %s""",
                (new_account_id, order["id"])
            )

            # 记录替换历史
            _execute(db,
                """INSERT INTO replacements (order_id, old_account_id, new_account_id, reason)
                   VALUES (%s, %s, %s, %s)""",
                (order["id"], old_account_id, new_account_id,
                 f"旧账号校验: {old_verify_classification}, 新账号校验: {new_verify_classification}")
            )

            # 记录新账号校验结果
            _execute(db,
                """INSERT INTO verifications (account_id, order_id, verification_type, result, details, verified_at)
                   VALUES (%s, %s, 'replace_new', %s, %s, %s)""",
                (new_account_id, order["id"], new_verify_classification, new_verify_details, to_db_datetime(utc_now()))
            )

            # 标记旧账号
            _execute(db, "UPDATE accounts SET status = 'blocked' WHERE id = %s", (old_account_id,))

            # 解析token数据
            token_data = {}
            if new_account["token_data"]:
                try:
                    token_data = json.loads(new_account["token_data"])
                except:
                    pass

            results.append({
                "cdk": cdk_code,
                "success": True,
                "order_id": order["id"],
                "new_account": {
                    "email": new_account["email"],
                    "access_token": new_account["access_token"],
                    "refresh_token": new_account["refresh_token"],
                    "id_token": new_account["id_token"],
                    "token_data": token_data,
                },
                "replacement_count": order["replacement_count"] + 1,
            })
            success_count += 1

        except Exception as e:
            results.append({"cdk": cdk_code, "success": False, "error": str(e)})
            failed_count += 1

    db.commit()

    return jsonify({
        "success": True,
        "total": len(cdks),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    })


# ==================== 管理员路由 ====================


@app.route("/admin")
def admin_index():
    """管理员首页"""
    if not session.get("admin_logged_in"):
        return redirect(url_for("admin_login"))
    return render_template("admin.html")


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    """管理员登录"""
    if request.method == "POST":
        data = request.get_json() if request.is_json else request.form
        username = data.get("username", "")
        password = data.get("password", "")

        if username == CONFIG["admin"]["username"] and password == CONFIG["admin"]["password"]:
            session["admin_logged_in"] = True
            if request.is_json:
                return jsonify({"success": True})
            return redirect(url_for("admin_index"))

        if request.is_json:
            return jsonify({"success": False, "error": "用户名或密码错误"}), 401
        return render_template("admin_login.html", error="用户名或密码错误")

    return render_template("admin_login.html")


@app.route("/admin/logout")
def admin_logout():
    """管理员登出"""
    session.pop("admin_logged_in", None)
    return redirect(url_for("admin_login"))


@app.route("/api/admin/stats")
@admin_required
def admin_stats():
    """获取统计信息"""
    db = get_db()

    total_cdks = _execute(db, "SELECT COUNT(*) as cnt FROM cdks").fetchone()["cnt"]
    unused_cdks = _execute(db, "SELECT COUNT(*) as cnt FROM cdks WHERE status = 'unused'").fetchone()["cnt"]
    used_cdks = _execute(db, "SELECT COUNT(*) as cnt FROM cdks WHERE status = 'used'").fetchone()["cnt"]

    total_accounts = _execute(db, "SELECT COUNT(*) as cnt FROM accounts").fetchone()["cnt"]
    available_accounts = _execute(db,
        "SELECT COUNT(*) as cnt FROM accounts WHERE status = 'available'"
    ).fetchone()["cnt"]
    blocked_accounts = _execute(db,
        "SELECT COUNT(*) as cnt FROM accounts WHERE status = 'blocked'"
    ).fetchone()["cnt"]

    # 空闲账号：可用且未被订单占用
    idle_accounts = _execute(db,
        "SELECT COUNT(*) as cnt FROM accounts WHERE status = 'available' "
        "AND id NOT IN (SELECT DISTINCT account_id FROM orders WHERE account_id IS NOT NULL)"
    ).fetchone()["cnt"]
    idle_accounts_by_created_date_rows = _execute(
        db,
        _sql_admin_stats_idle_accounts_by_created_date(),
    ).fetchall()
    idle_accounts_by_created_date = [dict(row) for row in idle_accounts_by_created_date_rows]

    total_orders = _execute(db, "SELECT COUNT(*) as cnt FROM orders").fetchone()["cnt"]
    active_orders = _execute(db,
        "SELECT COUNT(*) as cnt FROM orders WHERE status = 'active'"
    ).fetchone()["cnt"]

    # 质保内订单：状态为active且质保未过期
    warranty_orders = _execute(db,
        "SELECT COUNT(*) as cnt FROM orders WHERE status = 'active' AND warranty_expires_at > %s",
        (to_db_datetime(utc_now()),)
    ).fetchone()["cnt"]

    # 极限账号数：质保内订单都用完替换次数所需的额外账号数
    max_replacements = int(get_setting("max_replacements", "3"))
    warranty_order_replacements = _execute(db,
        "SELECT COALESCE(SUM(replacement_count), 0) as total FROM orders "
        "WHERE status = 'active' AND warranty_expires_at > %s",
        (to_db_datetime(utc_now()),)
    ).fetchone()["total"]
    # 每个质保内订单最多可替换 max_replacements 次，已替换 replacement_count 次
    # 极限还需账号数 = 质保内订单数 * max_replacements - 已替换总次数
    max_needed_accounts = warranty_orders * max_replacements - warranty_order_replacements

    total_replacements = _execute(db, "SELECT COUNT(*) as cnt FROM replacements").fetchone()["cnt"]

    return jsonify({
        "cdks": {
            "total": total_cdks,
            "unused": unused_cdks,
            "used": used_cdks,
        },
        "accounts": {
            "total": total_accounts,
            "available": available_accounts,
            "blocked": blocked_accounts,
            "idle": idle_accounts,
            "idle_by_created_date": idle_accounts_by_created_date,
        },
        "orders": {
            "total": total_orders,
            "active": active_orders,
            "in_warranty": warranty_orders,
        },
        "max_needed_accounts": max_needed_accounts,
        "replacements": total_replacements,
    })


@app.route("/api/admin/cdks", methods=["GET", "POST", "DELETE"])
@admin_required
def admin_cdks():
    """CDK管理"""
    db = get_db()

    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 20))
        status = request.args.get("status", "")
        search = request.args.get("search", "").strip()

        offset = (page - 1) * per_page

        query = "SELECT * FROM cdks"
        params = []
        where_conditions = []

        if status:
            where_conditions.append("status = %s")
            params.append(status)
        if search:
            where_conditions.append(_sql_like("code"))
            params.append(f"%{search}%")

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([per_page, offset])

        cdks = _execute(db, query, params).fetchall()

        count_query = "SELECT COUNT(*) as cnt FROM cdks"
        count_params = []
        if where_conditions:
            count_query += " WHERE " + " AND ".join(where_conditions)
            count_params = params[:-2]
            total = _execute(db, count_query, count_params).fetchone()["cnt"]
        else:
            total = _execute(db, count_query).fetchone()["cnt"]

        return jsonify({
            "cdks": [dict(cdk) for cdk in cdks],
            "total": total,
            "page": page,
            "per_page": per_page,
        })

    elif request.method == "POST":
        data = request.get_json()
        count = int(data.get("count", 1))
        account_ids = data.get("account_ids", [])

        if count < 1 or count > 1000:
            return jsonify({"success": False, "error": "数量必须在1-1000之间"}), 400

        results = create_cdks(count, account_ids if account_ids else None)
        return jsonify({
            "success": True,
            "created": [r for r in results if r["success"]],
            "failed": [r for r in results if not r["success"]],
        })

    elif request.method == "DELETE":
        data = request.get_json()
        cdk_ids = data.get("ids", [])

        if not cdk_ids:
            return jsonify({"success": False, "error": "请选择要删除的CDK"}), 400

        placeholders = ",".join("%s" for _ in cdk_ids)
        _execute(db,
            f"DELETE FROM cdks WHERE id IN ({placeholders}) AND status = 'unused'",
            cdk_ids
        )
        db.commit()

        return jsonify({"success": True, "message": "删除成功"})


@app.route("/api/admin/cdks/<int:cdk_id>/bind", methods=["POST"])
@admin_required
def bind_cdk_account(cdk_id):
    """绑定CDK到账号"""
    data = request.get_json()
    account_id = data.get("account_id")

    db = get_db()
    cdk = _execute(db, "SELECT * FROM cdks WHERE id = %s", (cdk_id,)).fetchone()

    if not cdk:
        return jsonify({"success": False, "error": "CDK不存在"}), 404

    if cdk["status"] == "used":
        return jsonify({"success": False, "error": "CDK已被使用，无法绑定"}), 400

    if account_id:
        account = _execute(db, "SELECT * FROM accounts WHERE id = %s", (account_id,)).fetchone()
        if not account:
            return jsonify({"success": False, "error": "账号不存在"}), 404

    _execute(db, "UPDATE cdks SET account_id = %s WHERE id = %s", (account_id, cdk_id))
    db.commit()

    return jsonify({"success": True, "message": "绑定成功"})


@app.route("/api/admin/accounts/upload", methods=["POST"])
@admin_required
def upload_accounts_json():
    """通过上传JSON文件批量导入账号"""
    files = request.files.getlist("files")

    if not files or all(f.filename == "" for f in files):
        return jsonify({"success": False, "error": "未选择文件"}), 400

    db = get_db()
    all_results = []
    file_stats = []
    total_imported = 0
    total_failed = 0

    for file in files:
        if file.filename == "":
            continue
        if not file.filename.endswith(".json"):
            file_stats.append({"file": file.filename, "error": "不是JSON文件"})
            continue

        try:
            content = file.read().decode("utf-8")
            data = json.loads(content)
        except json.JSONDecodeError as e:
            file_stats.append({"file": file.filename, "error": f"JSON解析错误: {str(e)}"})
            continue
        except Exception as e:
            file_stats.append({"file": file.filename, "error": f"读取失败: {str(e)}"})
            continue

        # 支持单个对象或数组
        accounts_data = data if isinstance(data, list) else [data]

        file_imported = 0
        file_failed = 0
        for acc in accounts_data:
            email = acc.get("email", "").strip()
            if not email:
                # 尝试从其他字段获取邮箱
                email = acc.get("identity_email", "") or acc.get("username", "")
                if isinstance(email, str):
                    email = email.strip()
                if not email:
                    all_results.append({"file": file.filename, "email": "(未知)", "success": False, "error": "缺少邮箱"})
                    file_failed += 1
                    continue

            token_payload = build_account_token_payload(acc)
            token_data = json.dumps(token_payload) if token_payload else None

            access_token = _pick_first_nonempty(acc.get("access_token"), token_payload.get("access_token"))
            refresh_token = _pick_first_nonempty(acc.get("refresh_token"), token_payload.get("refresh_token"))
            id_token = _pick_first_nonempty(acc.get("id_token"), token_payload.get("id_token"))

            # 检查邮箱是否已存在
            existing = _execute(db, "SELECT id FROM accounts WHERE email = %s", (email,)).fetchone()
            if existing:
                all_results.append({"file": file.filename, "email": email, "success": False, "error": "邮箱已存在"})
                file_failed += 1
                continue

            try:
                _execute(db,
                    """INSERT INTO accounts
                       (email, access_token, refresh_token, id_token, token_data, status)
                       VALUES (%s, %s, %s, %s, %s, 'unknown')""",
                    (
                        email,
                        access_token,
                        refresh_token,
                        id_token,
                        token_data,
                    )
                )
                all_results.append({"file": file.filename, "email": email, "success": True})
                file_imported += 1
            except Exception as e:
                all_results.append({"file": file.filename, "email": email, "success": False, "error": str(e)})
                file_failed += 1

        file_stats.append({"file": file.filename, "imported": file_imported, "failed": file_failed})
        total_imported += file_imported
        total_failed += file_failed

    db.commit()
    return jsonify({
        "success": True,
        "files_processed": len([f for f in file_stats if "error" not in f]),
        "total_imported": total_imported,
        "total_failed": total_failed,
        "file_stats": file_stats,
        "imported": [r for r in all_results if r["success"]],
        "failed": [r for r in all_results if not r["success"]],
    })


@app.route("/api/admin/accounts", methods=["GET", "POST", "DELETE"])
@admin_required
def admin_accounts():
    """账号管理"""
    db = get_db()

    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 20))
        status = request.args.get("status", "")
        email_search = request.args.get("email", "").strip()

        offset = (page - 1) * per_page

        query = "SELECT id, email, status, last_verified_at, created_at FROM accounts"
        params = []
        where_conditions = []

        if status:
            where_conditions.append("status = %s")
            params.append(status)
        if email_search:
            where_conditions.append(_sql_like("email"))
            params.append(f"%{email_search}%")

        if where_conditions:
            query += " WHERE " + " AND ".join(where_conditions)
        query += " ORDER BY created_at DESC LIMIT %s OFFSET %s"
        params.extend([per_page, offset])

        accounts = _execute(db, query, params).fetchall()

        count_query = "SELECT COUNT(*) as cnt FROM accounts"
        count_params = []
        if where_conditions:
            count_query += " WHERE " + " AND ".join(where_conditions)
            count_params = params[:-2]  # 排除 LIMIT 和 OFFSET 参数
            total = _execute(db, count_query, count_params).fetchone()["cnt"]
        else:
            total = _execute(db, count_query).fetchone()["cnt"]

        return jsonify({
            "accounts": [dict(acc) for acc in accounts],
            "total": total,
            "page": page,
            "per_page": per_page,
        })

    elif request.method == "POST":
        data = request.get_json()

        # 支持批量导入
        accounts_data = data.get("accounts", [])
        if not accounts_data and data.get("email"):
            accounts_data = [data]

        results = []
        for acc in accounts_data:
            email = acc.get("email", "").strip()
            if not email:
                continue

            token_payload = build_account_token_payload(acc)
            token_data = json.dumps(token_payload) if token_payload else None

            access_token = _pick_first_nonempty(acc.get("access_token"), token_payload.get("access_token"))
            refresh_token = _pick_first_nonempty(acc.get("refresh_token"), token_payload.get("refresh_token"))
            id_token = _pick_first_nonempty(acc.get("id_token"), token_payload.get("id_token"))

            # 检查邮箱是否已存在
            existing = _execute(db, "SELECT id FROM accounts WHERE email = %s", (email,)).fetchone()
            if existing:
                results.append({"email": email, "success": False, "error": "邮箱已存在"})
                continue

            try:
                _execute(db,
                    """INSERT INTO accounts
                       (email, access_token, refresh_token, id_token, token_data, status)
                       VALUES (%s, %s, %s, %s, %s, 'unknown')""",
                    (
                        email,
                        access_token,
                        refresh_token,
                        id_token,
                        token_data,
                    )
                )
                results.append({"email": email, "success": True})
            except Exception as e:
                results.append({"email": email, "success": False, "error": str(e)})

        db.commit()
        return jsonify({
            "success": True,
            "imported": [r for r in results if r["success"]],
            "failed": [r for r in results if not r["success"]],
        })

    elif request.method == "DELETE":
        data = request.get_json(silent=True) or {}
        raw_account_ids = data.get("ids", [])

        account_ids = []
        for account_id in raw_account_ids:
            try:
                account_ids.append(int(account_id))
            except (TypeError, ValueError):
                continue

        account_ids = list(dict.fromkeys(account_ids))

        if not account_ids:
            return jsonify({"success": False, "error": "请选择要删除的账号"}), 400

        placeholders = ",".join("%s" for _ in account_ids)
        linked_rows = _execute(
            db,
            f"""SELECT DISTINCT a.id, a.email
               FROM accounts a
               INNER JOIN orders o ON o.account_id = a.id
               WHERE a.id IN ({placeholders})""",
            account_ids,
        ).fetchall()
        linked_accounts = [dict(row) for row in linked_rows]
        linked_account_ids = {row["id"] for row in linked_accounts}

        deletable_ids = [account_id for account_id in account_ids if account_id not in linked_account_ids]

        deleted_count = 0
        if deletable_ids:
            delete_placeholders = ",".join("%s" for _ in deletable_ids)
            try:
                cursor = _execute(db, f"DELETE FROM accounts WHERE id IN ({delete_placeholders})", deletable_ids)
            except _INTEGRITY_ERRORS:
                db.rollback()
                return jsonify({
                    "success": False,
                    "error": "删除失败，部分账号仍有关联订单，请刷新后重试",
                }), 409
            deleted_count = cursor.rowcount

        db.commit()

        if linked_accounts:
            blocked_emails = [acc["email"] for acc in linked_accounts if acc.get("email")]
            blocked_preview = "、".join(blocked_emails[:5])
            blocked_message = f"，如 {blocked_preview}" if blocked_preview else ""
            return jsonify({
                "success": deleted_count > 0,
                "message": f"已删除 {deleted_count} 个账号，{len(linked_accounts)} 个账号因已有订单无法删除{blocked_message}",
                "deleted_count": deleted_count,
                "blocked_count": len(linked_accounts),
                "blocked_accounts": linked_accounts,
            }), 200 if deleted_count > 0 else 409

        return jsonify({"success": True, "message": f"成功删除 {deleted_count} 个账号", "deleted_count": deleted_count})


@app.route("/api/admin/accounts/<int:account_id>/verify", methods=["POST"])
@admin_required
def verify_account(account_id):
    """验证账号状态"""
    result = check_account_status(account_id)
    return jsonify(result)


@app.route("/api/admin/accounts/<int:account_id>/warmup", methods=["POST"])
@admin_required
def warmup_single_account(account_id):
    """预热单个未出单账号。"""
    result = warmup_account(account_id)
    status_code = 200 if result.get("success") else 400
    if result.get("status") == "request_failed":
        status_code = 502
    return jsonify(result), status_code


@app.route("/api/admin/accounts/<int:account_id>/quota", methods=["POST"])
@admin_required
def query_account_balance(account_id):
    """查询账号余额/额度"""
    result = query_account_quota(account_id)
    status_code = 200 if result.get("success") else 400
    return jsonify(result), status_code


@app.route("/api/admin/accounts/verify-all", methods=["POST"])
@admin_required
def verify_all_accounts():
    """批量验证账号（SSE流式返回进度）"""
    data = request.get_json() or {}
    only_unknown = data.get("only_unknown", False)

    # 使用连接池直接获取连接查询账号列表
    db = _get_pool().connection()
    try:
        if only_unknown:
            accounts = _execute(db, "SELECT id, email FROM accounts WHERE status = 'unknown'").fetchall()
        else:
            accounts = _execute(db, "SELECT id, email FROM accounts WHERE status IN ('available', 'unknown')").fetchall()
        # 转换为列表，因为连接会在 generator 外关闭
        accounts_list = [dict(acc) for acc in accounts]
    finally:
        db.close()

    total = len(accounts_list)

    def generate():
        if total == 0:
            yield f"data: {json.dumps({'done': True, 'total': 0, 'blocked': 0, 'unknown': 0, 'available': 0}, ensure_ascii=False)}\n\n"
            return

        blocked_count = 0
        available_count = 0
        unknown_count = 0

        for i, acc in enumerate(accounts_list, 1):
            # 每次迭代获取新的数据库连接
            db = _get_pool().connection()
            try:
                result = check_account_status(acc["id"], db)
            except Exception as e:
                result = {"classification": "unknown", "error": str(e)}
            finally:
                db.close()

            classification = result.get("classification", "unknown")

            if classification == "blocked":
                blocked_count += 1
            elif classification == "available":
                available_count += 1
            else:
                unknown_count += 1

            # 发送进度事件
            progress_data = {
                "current": i,
                "total": total,
                "email": acc["email"],
                "classification": classification,
                "blocked": blocked_count,
                "available": available_count,
                "unknown": unknown_count,
            }
            yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"

        # 发送完成事件
        complete_data = {
            "done": True,
            "total": total,
            "blocked": blocked_count,
            "available": available_count,
            "unknown": unknown_count,
        }
        yield f"data: {json.dumps(complete_data, ensure_ascii=False)}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@app.route("/api/admin/accounts/warmup-all", methods=["POST"])
@admin_required
def warmup_all_accounts():
    """批量预热所有未出单且未封禁账号。"""
    db = _get_pool().connection()
    try:
        accounts = _execute(
            db,
            """SELECT a.id, a.email, a.status
               FROM accounts a
               LEFT JOIN orders o ON o.account_id = a.id
               WHERE o.id IS NULL AND a.status IN ('available', 'unknown')
               ORDER BY a.created_at DESC""",
        ).fetchall()
        accounts_list = [dict(acc) for acc in accounts]
    finally:
        db.close()

    total = len(accounts_list)

    def generate():
        if total == 0:
            yield f"data: {json.dumps({'done': True, 'total': 0, 'success': 0, 'failed': 0}, ensure_ascii=False)}\n\n"
            return

        success_count = 0
        failed_count = 0

        for i, acc in enumerate(accounts_list, 1):
            db = _get_pool().connection()
            try:
                result = warmup_account(acc["id"], db)
            except Exception as e:
                result = {"success": False, "error": str(e), "status": "exception"}
            finally:
                db.close()

            if result.get("success"):
                success_count += 1
            else:
                failed_count += 1

            progress_data = {
                "current": i,
                "total": total,
                "email": acc["email"],
                "success": success_count,
                "failed": failed_count,
                "ok": bool(result.get("success")),
                "status": result.get("status"),
                "error": result.get("error"),
            }
            yield f"data: {json.dumps(progress_data, ensure_ascii=False)}\n\n"

        complete_data = {
            "done": True,
            "total": total,
            "success": success_count,
            "failed": failed_count,
        }
        yield f"data: {json.dumps(complete_data, ensure_ascii=False)}\n\n"

    return Response(
        stream_with_context(generate()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive",
        }
    )


@app.route("/api/admin/orders")
@admin_required
def admin_orders():
    """订单管理"""
    db = get_db()

    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    status = request.args.get("status", "")

    offset = (page - 1) * per_page

    query = """
        SELECT o.*, a.email as account_email, a.status as account_status
        FROM orders o
        LEFT JOIN accounts a ON o.account_id = a.id
    """
    params = []
    if status:
        query += " WHERE o.status = %s"
        params.append(status)
    query += " ORDER BY o.created_at DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    orders = _execute(db, query, params).fetchall()

    count_query = "SELECT COUNT(*) as cnt FROM orders"
    if status:
        count_query += " WHERE status = %s"
        total = _execute(db, count_query, [status]).fetchone()["cnt"]
    else:
        total = _execute(db, count_query).fetchone()["cnt"]

    # 动态判断订单状态（根据质保到期时间）
    now = utc_now()
    orders_list = []
    for order in orders:
        order_dict = dict(order)
        order_dict["warranty_expires_at"] = to_api_datetime(order_dict.get("warranty_expires_at"))
        order_dict["created_at"] = to_api_datetime(order_dict.get("created_at"))
        # 根据质保到期时间动态判断状态
        warranty_expires = order_dict.get("warranty_expires_at")
        if warranty_expires:
            try:
                expires_dt = to_utc_time(warranty_expires)
                if now >= expires_dt:
                    order_dict["effective_status"] = "expired"
                else:
                    order_dict["effective_status"] = "active"
            except:
                order_dict["effective_status"] = order_dict.get("status", "active")
        else:
            order_dict["effective_status"] = order_dict.get("status", "active")
        orders_list.append(order_dict)

    return jsonify({
        "orders": orders_list,
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@app.route("/api/admin/orders/<int:order_id>", methods=["DELETE"])
@admin_required
def delete_order(order_id):
    """删除订单并删除CDK"""
    db = get_db()

    # 获取订单信息
    order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    cdk_code = order["cdk_code"]
    account_id = order["account_id"]

    # 删除替换记录
    _execute(db, "DELETE FROM replacements WHERE order_id = %s", (order_id,))

    # 删除校验记录
    _execute(db, "DELETE FROM verifications WHERE order_id = %s", (order_id,))

    # 删除订单
    _execute(db, "DELETE FROM orders WHERE id = %s", (order_id,))

    # 删除CDK
    _execute(db, "DELETE FROM cdks WHERE code = %s", (cdk_code,))

    db.commit()

    return jsonify({
        "success": True,
        "message": f"订单 #{order_id} 和 CDK {cdk_code} 已删除",
        "deleted_cdk": cdk_code,
    })


@app.route("/api/admin/orders/batch-delete", methods=["POST"])
@admin_required
def delete_orders_batch():
    """批量删除订单并删除CDK"""
    data = request.get_json()
    order_ids = data.get("ids", [])

    if not order_ids:
        return jsonify({"success": False, "error": "请选择要删除的订单"}), 400

    if len(order_ids) > 100:
        return jsonify({"success": False, "error": "单次最多删除100个订单"}), 400

    db = get_db()
    results = []
    success_count = 0
    failed_count = 0

    for order_id in order_ids:
        try:
            order = _execute(db, "SELECT * FROM orders WHERE id = %s", (order_id,)).fetchone()
            if not order:
                results.append({"order_id": order_id, "success": False, "error": "订单不存在"})
                failed_count += 1
                continue

            cdk_code = order["cdk_code"]

            # 删除替换记录
            _execute(db, "DELETE FROM replacements WHERE order_id = %s", (order_id,))

            # 删除订单
            _execute(db, "DELETE FROM orders WHERE id = %s", (order_id,))

            # 删除CDK
            _execute(db, "DELETE FROM cdks WHERE code = %s", (cdk_code,))

            results.append({
                "order_id": order_id,
                "success": True,
                "cdk": cdk_code,
            })
            success_count += 1

        except Exception as e:
            results.append({"order_id": order_id, "success": False, "error": str(e)})
            failed_count += 1

    db.commit()

    return jsonify({
        "success": True,
        "total": len(order_ids),
        "success_count": success_count,
        "failed_count": failed_count,
        "results": results,
    })


@app.route("/api/admin/verifications")
@admin_required
def get_verifications():
    """获取提卡日志"""
    db = get_db()
    page = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 20))
    v_type = request.args.get("type", "")
    v_result = request.args.get("result", "")

    offset = (page - 1) * per_page

    query = """SELECT v.*, o.cdk_code
               FROM verifications v
               LEFT JOIN orders o ON v.order_id = o.id"""
    params = []
    where_conditions = []

    if v_type:
        where_conditions.append("v.verification_type = %s")
        params.append(v_type)
    if v_result:
        where_conditions.append("v.result = %s")
        params.append(v_result)

    if where_conditions:
        query += " WHERE " + " AND ".join(where_conditions)
    query += " ORDER BY v.verified_at DESC LIMIT %s OFFSET %s"
    params.extend([per_page, offset])

    verifications = _execute(db, query, params).fetchall()

    count_query = "SELECT COUNT(*) as cnt FROM verifications v"
    count_params = []
    if where_conditions:
        count_query += " WHERE " + " AND ".join(where_conditions)
        count_params = params[:-2]
        total = _execute(db, count_query, count_params).fetchone()["cnt"]
    else:
        total = _execute(db, count_query).fetchone()["cnt"]

    return jsonify({
        "verifications": [dict(v) for v in verifications],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@app.route("/api/admin/orders/expired-blocked", methods=["DELETE"])
@admin_required
def delete_expired_blocked_orders():
    """删除过期且账号封禁的订单"""
    db = get_db()
    now = to_db_datetime(utc_now())

    # 查找过期且账号封禁的订单
    orders = _execute(db, """
        SELECT o.id, o.cdk_code FROM orders o
        JOIN accounts a ON o.account_id = a.id
        WHERE o.warranty_expires_at < %s AND a.status = 'blocked'
    """, (now,)).fetchall()

    deleted_count = 0
    for order in orders:
        try:
            # 删除替换记录
            _execute(db, "DELETE FROM replacements WHERE order_id = %s", (order["id"],))
            # 删除校验记录
            _execute(db, "DELETE FROM verifications WHERE order_id = %s", (order["id"],))
            # 删除订单
            _execute(db, "DELETE FROM orders WHERE id = %s", (order["id"],))
            # 删除CDK
            _execute(db, "DELETE FROM cdks WHERE code = %s", (order["cdk_code"],))
            deleted_count += 1
        except Exception as e:
            app.logger.error(f"删除订单 {order['id']} 失败: {e}")

    db.commit()
    return jsonify({"success": True, "deleted_count": deleted_count})


@app.route("/api/admin/accounts/blocked-no-order", methods=["DELETE"])
@admin_required
def delete_blocked_no_order_accounts():
    """删除无订单且封禁的账号"""
    db = get_db()

    # 查找封禁且没有关联订单的账号
    accounts = _execute(db, """
        SELECT a.id FROM accounts a
        LEFT JOIN orders o ON a.id = o.account_id
        WHERE a.status = 'blocked' AND o.id IS NULL
    """).fetchall()

    deleted_count = 0
    for account in accounts:
        try:
            _execute(db, "DELETE FROM accounts WHERE id = %s", (account["id"],))
            deleted_count += 1
        except Exception as e:
            app.logger.error(f"删除账号 {account['id']} 失败: {e}")

    db.commit()
    return jsonify({"success": True, "deleted_count": deleted_count})


@app.route("/api/admin/settings", methods=["GET", "POST"])
@admin_required
def admin_settings():
    """系统设置"""
    db = get_db()

    if request.method == "GET":
        settings = _execute(db, "SELECT * FROM settings").fetchall()
        return jsonify({key: value for key, value in [(s["key"], s["value"]) for s in settings]})

    elif request.method == "POST":
        data = request.get_json()
        for key, value in data.items():
            set_setting(key, str(value))
        return jsonify({"success": True, "message": "设置已保存"})


@app.route("/api/admin/import-from-data", methods=["POST"])
@admin_required
def import_from_data():
    """从data目录导入账号"""
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data")

    if not os.path.exists(data_dir):
        return jsonify({"success": False, "error": "data目录不存在"}), 400

    imported = []
    failed = []

    for filename in os.listdir(data_dir):
        if filename.endswith(".json"):
            filepath = os.path.join(data_dir, filename)
            try:
                with open(filepath, "r", encoding="utf-8") as f:
                    account_data = json.load(f)

                # 提取token信息
                final = account_data.get("final", {})
                tokens = final.get("tokens", {})

                email = (
                    account_data.get("email")
                    or final.get("identity_email")
                    or filename.replace(".json", "")
                )

                db = get_db()

                # 检查邮箱是否已存在
                existing = _execute(db, "SELECT id FROM accounts WHERE email = %s", (email,)).fetchone()
                if existing:
                    failed.append({"email": email, "error": "邮箱已存在"})
                    continue

                try:
                    _execute(db,
                        """INSERT INTO accounts
                           (email, access_token, refresh_token, id_token, token_data, status)
                           VALUES (%s, %s, %s, %s, %s, 'available')""",
                        (
                            email,
                            tokens.get("access_token"),
                            tokens.get("refresh_token"),
                            tokens.get("id_token"),
                            json.dumps(account_data),
                        )
                    )
                    db.commit()
                    imported.append(email)
                except Exception as e:
                    failed.append({"email": email, "error": str(e)})
            except Exception as e:
                failed.append({"file": filename, "error": str(e)})

    return jsonify({
        "success": True,
        "imported": imported,
        "failed": failed,
    })


# ==================== 初始化 ====================

_db_initialized = False


@app.before_request
def _ensure_db_initialized():
    """首次请求时初始化数据库表结构（延迟到 MySQL 可用时）"""
    global _db_initialized
    if not _db_initialized:
        try:
            init_db()
            _db_initialized = True
            print("[DB] Database initialized successfully", flush=True)
        except Exception as e:
            print(f"[DB] init_db failed: {e}", flush=True)


if __name__ == "__main__":
    print(f"Kiro 发卡平台启动中...")
    print(f"访问地址: http://{CONFIG['server']['host']}:{CONFIG['server']['port']}")
    print(f"管理后台: http://{CONFIG['server']['host']}:{CONFIG['server']['port']}/admin")
    app.run(
        host=CONFIG["server"]["host"],
        port=CONFIG["server"]["port"],
        debug=CONFIG["server"]["debug"],
    )
