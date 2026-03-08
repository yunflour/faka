#!/usr/bin/env python3
"""
Kiro 在线发卡平台
支持CDK提卡、账号校验、质保替换功能
"""

import io
import json
import os
import random
import re
import secrets
import sqlite3
import string
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Optional

import requests
from flask import Flask, g, jsonify, redirect, render_template, request, send_file, session, url_for

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
        "path": "faka.db",
    },
    "cdk": {
        "length": 16,
        "prefix": "KIRO",
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

CONFIG["database"]["path"] = _env_str("DATABASE_PATH", str(CONFIG["database"]["path"]))

CONFIG["cdk"]["length"] = _env_int("CDK_LENGTH", int(CONFIG["cdk"]["length"]))
CONFIG["cdk"]["prefix"] = _env_str("CDK_PREFIX", str(CONFIG["cdk"]["prefix"]))

app = Flask(__name__)
app.secret_key = CONFIG["server"]["secret_key"]

DATABASE = os.path.join(os.path.dirname(__file__), CONFIG["database"]["path"])


# ==================== 数据库相关 ====================


def get_db():
    """获取数据库连接"""
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = sqlite3.connect(DATABASE)
        db.row_factory = sqlite3.Row
    return db


@app.teardown_appcontext
def close_connection(exception):
    """关闭数据库连接"""
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


def init_db():
    """初始化数据库"""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()

    # CDK表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS cdks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL,
            account_id INTEGER,
            status TEXT DEFAULT 'unused',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            used_at TIMESTAMP,
            used_by TEXT
        )
    """)

    # 账号表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT NOT NULL,
            access_token TEXT,
            refresh_token TEXT,
            id_token TEXT,
            token_data TEXT,
            status TEXT DEFAULT 'available',
            last_verified_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 订单表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            cdk_code TEXT NOT NULL,
            account_id INTEGER NOT NULL,
            user_ip TEXT,
            warranty_days INTEGER DEFAULT 7,
            warranty_expires_at TIMESTAMP,
            replacement_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (account_id) REFERENCES accounts(id),
            FOREIGN KEY (cdk_code) REFERENCES cdks(code)
        )
    """)

    # 替换记录表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS replacements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id INTEGER NOT NULL,
            old_account_id INTEGER NOT NULL,
            new_account_id INTEGER NOT NULL,
            reason TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (order_id) REFERENCES orders(id)
        )
    """)

    # 系统设置表
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        )
    """)

    # 初始化默认设置
    default_settings = [
        ("warranty_days", str(CONFIG["warranty"]["default_days"])),
        ("max_replacements", str(CONFIG["warranty"]["max_replacements"])),
        ("site_title", "Kiro 发卡平台"),
        ("site_notice", "欢迎使用Kiro发卡平台"),
    ]
    for key, value in default_settings:
        cursor.execute(
            "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value)
        )

    conn.commit()
    conn.close()


def get_setting(key: str, default: str = "") -> str:
    """获取系统设置"""
    db = get_db()
    row = db.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str):
    """设置系统设置"""
    db = get_db()
    db.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, value)
    )
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


def resolve_account_verification_context(account: sqlite3.Row) -> dict[str, str]:
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


def check_account_status(account_id: int) -> dict[str, Any]:
    """检查账号状态"""
    db = get_db()
    account = db.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()

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
        db.execute(
            "UPDATE accounts SET last_verified_at = ? WHERE id = ?",
            (datetime.now().isoformat(), account_id),
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
            datetime.now().isoformat(),
            account_id,
        )
        db.execute(
            """UPDATE accounts
               SET access_token = ?, refresh_token = ?, id_token = ?, token_data = ?, status = ?, last_verified_at = ?
               WHERE id = ?""",
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
            datetime.now().isoformat(),
            account_id,
        )
        db.execute(
            """UPDATE accounts
               SET access_token = ?, refresh_token = ?, id_token = ?, token_data = ?, last_verified_at = ?
               WHERE id = ?""",
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
            db.execute(
                "INSERT INTO cdks (code, account_id, status) VALUES (?, ?, ?)",
                (code, account_id, "unused"),
            )
            results.append({"code": code, "success": True})
        except sqlite3.IntegrityError:
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
    cdk = db.execute("SELECT * FROM cdks WHERE code = ?", (cdk_code,)).fetchone()

    if not cdk:
        return jsonify({"success": False, "error": "CDK不存在"}), 404

    if cdk["status"] == "used":
        # 检查是否有订单
        order = db.execute(
            "SELECT * FROM orders WHERE cdk_code = ?", (cdk_code,)
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
    if not account_id:
        # 分配一个可用账号
        available_account = db.execute(
            "SELECT * FROM accounts WHERE status = 'available' AND id NOT IN "
            "(SELECT account_id FROM orders WHERE status = 'active') LIMIT 1"
        ).fetchone()
        if not available_account:
            return jsonify({"success": False, "error": "暂无可用账号"}), 400
        account_id = available_account["id"]

    # 获取账号信息
    account = db.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
    if not account:
        return jsonify({"success": False, "error": "绑定的账号不存在"}), 400

    if account["status"] == "blocked":
        return jsonify({"success": False, "error": "绑定的账号已被封禁"}), 400

    # 获取质保天数
    warranty_days = int(get_setting("warranty_days", "7"))
    warranty_expires = datetime.now() + timedelta(days=warranty_days)

    # 创建订单
    db.execute(
        """INSERT INTO orders
           (cdk_code, account_id, user_ip, warranty_days, warranty_expires_at, status)
           VALUES (?, ?, ?, ?, ?, 'active')""",
        (cdk_code, account_id, request.remote_addr, warranty_days, warranty_expires.isoformat()),
    )

    # 更新CDK状态
    db.execute(
        "UPDATE cdks SET status = 'used', used_at = ?, used_by = ?, account_id = ? WHERE code = ?",
        (datetime.now().isoformat(), request.remote_addr, account_id, cdk_code),
    )

    db.commit()

    # 获取订单ID
    order = db.execute("SELECT * FROM orders WHERE cdk_code = ?", (cdk_code,)).fetchone()

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
    order = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = db.execute(
        "SELECT * FROM accounts WHERE id = ?", (order["account_id"],)
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
            "warranty_expires_at": order["warranty_expires_at"],
            "replacement_count": order["replacement_count"],
            "created_at": order["created_at"],
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
    """下载订单账号JSON，支持重复下载。"""
    db = get_db()
    order = db.execute("SELECT * FROM orders WHERE id = ?", (order_id,)).fetchone()
    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = db.execute("SELECT * FROM accounts WHERE id = ?", (order["account_id"],)).fetchone()
    if not account:
        return jsonify({"success": False, "error": "账号不存在"}), 404

    token_data = _parse_token_data(account["token_data"])

    # 以原始导入结构为准，避免导出结构漂移。
    if token_data:
        payload = dict(token_data)
    else:
        payload = {}

    # 若存在原始 token_data，严格原样导出，避免与导入结构不一致。
    # 仅在 token_data 为空的旧数据场景下，才做最小兜底。
    if not payload:
        payload = {
            "access_token": account["access_token"],
            "refresh_token": account["refresh_token"],
            "email": account["email"],
            "profile_arn": CONFIG["verification"].get("profile_arn"),
            "region": CONFIG["verification"].get("idc_region"),
        }
        if account["id_token"]:
            payload["id_token"] = account["id_token"]

    content = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    email_for_name = re.sub(r"[^A-Za-z0-9@._-]+", "_", str(account["email"] or "unknown").strip())
    filename = f"{email_for_name}_{order_id}.json"
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
    order = db.execute("SELECT * FROM orders WHERE cdk_code = ?", (cdk_code,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    account = db.execute(
        "SELECT * FROM accounts WHERE id = ?", (order["account_id"],)
    ).fetchone()

    # 检查账号状态
    account_check = check_account_status(order["account_id"]) if account else None

    # 检查是否在质保期内
    warranty_expires = datetime.fromisoformat(order["warranty_expires_at"])
    in_warranty = datetime.now() < warranty_expires

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
            "warranty_expires_at": order["warranty_expires_at"],
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
    order = db.execute("SELECT * FROM orders WHERE cdk_code = ?", (cdk_code,)).fetchone()

    if not order:
        return jsonify({"success": False, "error": "订单不存在"}), 404

    # 检查质保期
    warranty_expires = datetime.fromisoformat(order["warranty_expires_at"])
    if datetime.now() >= warranty_expires:
        return jsonify({"success": False, "error": "质保已过期"}), 400

    # 检查替换次数
    max_replacements = int(get_setting("max_replacements", "3"))
    if order["replacement_count"] >= max_replacements:
        return jsonify({"success": False, "error": "已超出最大替换次数"}), 400

    # 检查账号是否真的被封禁
    account_check = check_account_status(order["account_id"])
    if account_check.get("classification") == "available":
        return jsonify({"success": False, "error": "账号状态正常，无需替换"}), 400
    if account_check.get("classification") != "blocked":
        return jsonify({"success": False, "error": "当前无法明确判断账号已封禁，请稍后重试"}), 400

    old_account_id = order["account_id"]

    # 查找新的可用账号
    new_account = db.execute(
        """SELECT * FROM accounts WHERE status = 'available'
           AND id != ? AND id NOT IN (SELECT account_id FROM orders WHERE status = 'active')
           LIMIT 1""",
        (old_account_id,)
    ).fetchone()

    if not new_account:
        return jsonify({"success": False, "error": "暂无可用账号进行替换"}), 400

    new_account_id = new_account["id"]

    # 更新订单
    db.execute(
        """UPDATE orders SET account_id = ?, replacement_count = replacement_count + 1
           WHERE id = ?""",
        (new_account_id, order["id"])
    )

    # 记录替换历史
    db.execute(
        """INSERT INTO replacements (order_id, old_account_id, new_account_id, reason)
           VALUES (?, ?, ?, ?)""",
        (order["id"], old_account_id, new_account_id, "账号被封禁")
    )

    # 标记旧账号
    db.execute("UPDATE accounts SET status = 'blocked' WHERE id = ?", (old_account_id,))

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

    total_cdks = db.execute("SELECT COUNT(*) FROM cdks").fetchone()[0]
    unused_cdks = db.execute("SELECT COUNT(*) FROM cdks WHERE status = 'unused'").fetchone()[0]
    used_cdks = db.execute("SELECT COUNT(*) FROM cdks WHERE status = 'used'").fetchone()[0]

    total_accounts = db.execute("SELECT COUNT(*) FROM accounts").fetchone()[0]
    available_accounts = db.execute(
        "SELECT COUNT(*) FROM accounts WHERE status = 'available'"
    ).fetchone()[0]
    blocked_accounts = db.execute(
        "SELECT COUNT(*) FROM accounts WHERE status = 'blocked'"
    ).fetchone()[0]

    total_orders = db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    active_orders = db.execute(
        "SELECT COUNT(*) FROM orders WHERE status = 'active'"
    ).fetchone()[0]

    total_replacements = db.execute("SELECT COUNT(*) FROM replacements").fetchone()[0]

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
        },
        "orders": {
            "total": total_orders,
            "active": active_orders,
        },
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

        offset = (page - 1) * per_page

        query = "SELECT * FROM cdks"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([per_page, offset])

        cdks = db.execute(query, params).fetchall()

        count_query = "SELECT COUNT(*) FROM cdks"
        if status:
            count_query += " WHERE status = ?"
            total = db.execute(count_query, [status]).fetchone()[0]
        else:
            total = db.execute(count_query).fetchone()[0]

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

        placeholders = ",".join("?" * len(cdk_ids))
        db.execute(
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
    cdk = db.execute("SELECT * FROM cdks WHERE id = ?", (cdk_id,)).fetchone()

    if not cdk:
        return jsonify({"success": False, "error": "CDK不存在"}), 404

    if cdk["status"] == "used":
        return jsonify({"success": False, "error": "CDK已被使用，无法绑定"}), 400

    if account_id:
        account = db.execute("SELECT * FROM accounts WHERE id = ?", (account_id,)).fetchone()
        if not account:
            return jsonify({"success": False, "error": "账号不存在"}), 404

    db.execute("UPDATE cdks SET account_id = ? WHERE id = ?", (account_id, cdk_id))
    db.commit()

    return jsonify({"success": True, "message": "绑定成功"})


@app.route("/api/admin/accounts", methods=["GET", "POST", "DELETE"])
@admin_required
def admin_accounts():
    """账号管理"""
    db = get_db()

    if request.method == "GET":
        page = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 20))
        status = request.args.get("status", "")

        offset = (page - 1) * per_page

        query = "SELECT id, email, status, last_verified_at, created_at FROM accounts"
        params = []
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
        params.extend([per_page, offset])

        accounts = db.execute(query, params).fetchall()

        count_query = "SELECT COUNT(*) FROM accounts"
        if status:
            count_query += " WHERE status = ?"
            total = db.execute(count_query, [status]).fetchone()[0]
        else:
            total = db.execute(count_query).fetchone()[0]

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

            try:
                db.execute(
                    """INSERT INTO accounts
                       (email, access_token, refresh_token, id_token, token_data, status)
                       VALUES (?, ?, ?, ?, ?, 'available')""",
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
        data = request.get_json()
        account_ids = data.get("ids", [])

        if not account_ids:
            return jsonify({"success": False, "error": "请选择要删除的账号"}), 400

        placeholders = ",".join("?" * len(account_ids))
        db.execute(f"DELETE FROM accounts WHERE id IN ({placeholders})", account_ids)
        db.commit()

        return jsonify({"success": True, "message": "删除成功"})


@app.route("/api/admin/accounts/<int:account_id>/verify", methods=["POST"])
@admin_required
def verify_account(account_id):
    """验证账号状态"""
    result = check_account_status(account_id)
    return jsonify(result)


@app.route("/api/admin/accounts/verify-all", methods=["POST"])
@admin_required
def verify_all_accounts():
    """批量验证所有账号"""
    db = get_db()
    accounts = db.execute("SELECT id FROM accounts WHERE status = 'available'").fetchall()

    results = []
    for acc in accounts:
        result = check_account_status(acc["id"])
        results.append({
            "id": acc["id"],
            "valid": result["valid"],
            "classification": result.get("classification", "unknown"),
            "status": result["status"],
        })

    blocked_count = len([r for r in results if r.get("classification") == "blocked"])
    unknown_count = len([r for r in results if r.get("classification") == "unknown"])
    return jsonify({
        "success": True,
        "total": len(results),
        "blocked": blocked_count,
        "unknown": unknown_count,
        "results": results,
    })


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
        SELECT o.*, a.email as account_email
        FROM orders o
        LEFT JOIN accounts a ON o.account_id = a.id
    """
    params = []
    if status:
        query += " WHERE o.status = ?"
        params.append(status)
    query += " ORDER BY o.created_at DESC LIMIT ? OFFSET ?"
    params.extend([per_page, offset])

    orders = db.execute(query, params).fetchall()

    count_query = "SELECT COUNT(*) FROM orders"
    if status:
        count_query += " WHERE status = ?"
        total = db.execute(count_query, [status]).fetchone()[0]
    else:
        total = db.execute(count_query).fetchone()[0]

    return jsonify({
        "orders": [dict(order) for order in orders],
        "total": total,
        "page": page,
        "per_page": per_page,
    })


@app.route("/api/admin/settings", methods=["GET", "POST"])
@admin_required
def admin_settings():
    """系统设置"""
    db = get_db()

    if request.method == "GET":
        settings = db.execute("SELECT * FROM settings").fetchall()
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
                try:
                    db.execute(
                        """INSERT INTO accounts
                           (email, access_token, refresh_token, id_token, token_data, status)
                           VALUES (?, ?, ?, ?, ?, 'available')""",
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
                except sqlite3.IntegrityError:
                    failed.append({"email": email, "error": "已存在"})
            except Exception as e:
                failed.append({"file": filename, "error": str(e)})

    return jsonify({
        "success": True,
        "imported": imported,
        "failed": failed,
    })


# ==================== 初始化 ====================


if __name__ == "__main__":
    init_db()
    print(f"Kiro 发卡平台启动中...")
    print(f"访问地址: http://{CONFIG['server']['host']}:{CONFIG['server']['port']}")
    print(f"管理后台: http://{CONFIG['server']['host']}:{CONFIG['server']['port']}/admin")
    app.run(
        host=CONFIG["server"]["host"],
        port=CONFIG["server"]["port"],
        debug=CONFIG["server"]["debug"],
    )
