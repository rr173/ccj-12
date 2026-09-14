"""通知内容模板编排（traceable notification content templates）。

在既有通知事件、通道路由（notif_routing）、旧链路投递（approval_notification_deliveries）、
外部回执（receipts）与审计（events）之上，提供**按事件类型 × 通道生效、多语言、版本化**
的通知正文模板，并保证「发布后发送即固化」的可追溯性。

模板版本（notif_template_versions / notif_template_current）
- 运营先创建草稿（每个 event_type+channel 至多一份 draft），可反复编辑：声明必填变量、
  默认值、敏感变量、值类型，以及各语言标题/正文、可用语言与语言回退顺序；
- 发布前整份校验：占位符必须都有声明且语法为合法闭合的 {name}/{name|mask}
  （未闭合的 {name、孤立 } 以 unclosed_placeholder 拒绝）、声明变量的类型/默认值合法、
  敏感变量在每个用到它的占位符上必须脱敏（|mask）、每门语言标题/正文非空且占位符一致；
  校验失败落 rejected 记录（原因可查），当前指针不动；通过则发布为不可变版本（键内版本号
  单调递增）并推进 notif_template_current 指针；渲染侧另对占位符做同样的语法扫描，保证
  即便有绕过发布的脏数据，未闭合片段也只会阻断渲染，绝不会把原样正文发出去；
- event_type/channel 支持 '*' 通配，解析顺序：精确(event,channel) -> (event,'*') ->
  ('*',channel) -> ('*','*')；四处都没有则视为该通道未配置模板（沿用事件静态正文，
  存量行为完全不变）。

渲染与语言回退
- 入队时按接收人语言偏好（approval_contacts.language，缺省取 NOTIF_DEFAULT_LANGUAGE）
  选择语言，回退链 = 接收人语言 -> 模板声明 fallback_languages -> 系统缺省语言（去重，
  保留顺序），命中第一门有正文的语言；链上全无时以 missing_language 阻断；
- 变量缺失（无值也无默认）、类型不符（声明 int/number/bool 的值无法转换）、正文/标题
  超出通道长度上限、敏感变量原文出现在最终正文（未走 |mask）都不能发送，原因写入
  notif_template_render_failures（可按接收人/事件/通道/原因查询）；
- 预览端点对草稿/已发布版本用给定变量做只读渲染，返回每通道结果与全部校验/渲染错误，
  预览绝不产生任何发送效果。

固化（后续编辑不能改写已入队/已发送内容）
- 版本化路由任务：入队时为计划中每个通道渲染一次，模板版本、语言、回退链、变量快照
  （敏感变量只存脱敏值）、最终标题/正文/负载与正文哈希随 notif_send_tasks 固化，
  发送器只读计划项里的固化正文；渲染失败的通道从计划剔除，全部失败时任务落
  render_failed（终态，不派发、不自动重试），可由管理员在修好模板/变量后手动重试；
- 旧链路：approval_notification_deliveries 落盘的就是渲染后的 subject/body/payload，
  并固化模板版本、语言与正文哈希；全部通道渲染失败时事件不产生投递，失败可查。
  重试发送始终读取已落盘正文，不会重新渲染；
- 发布新版本只推进指针，只影响之后入队的任务；重复发布/重复渲染由任务 UNIQUE、
  未解除失败的部分唯一索引与「渲染只发生在入队事务内」保证不产生第二份发送效果。
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit
from . import notifications as notif
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications.templates")

# ---- 常量 -------------------------------------------------------------------

WILDCARD = "*"
CHANNEL_INBOX = "inbox"
TEMPLATE_CHANNELS = (notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK,
                     CHANNEL_INBOX, WILDCARD)
VARIABLE_TYPES = ("string", "int", "number", "bool")


def _routing():
    """惰性导入 notif_routing（避免与 notifications/routing 的模块级循环导入）。"""
    from . import notif_routing
    return notif_routing

STATUS_DRAFT = "draft"
STATUS_PUBLISHED = "published"
STATUS_REJECTED = "rejected"

# 渲染失败原因代码（notif_template_render_failures.reason_code）
REASON_MISSING_TEMPLATE = "missing_template"
REASON_MISSING_LANGUAGE = "missing_language"
REASON_MISSING_VARIABLE = "missing_variable"
REASON_TYPE_MISMATCH = "type_mismatch"
REASON_BODY_TOO_LONG = "body_too_long"
REASON_SUBJECT_TOO_LONG = "subject_too_long"
REASON_SENSITIVE_UNMASKED = "sensitive_unmasked"
REASON_UNCLOSED_PLACEHOLDER = "unclosed_placeholder"
REASON_RENDER_ERROR = "render_error"

# 发送任务的渲染失败终态（区别于通道故障的 quarantined：模板问题重试通道无意义，
# 必须管理员修复后显式重试渲染，worker 不自动派发）
TASK_RENDER_FAILED = "render_failed"

# 占位符语法：{name} 或 {name|mask}；不支持表达式/嵌套（避免模板注入）
# PLACEHOLDER_FIND_RE 只负责对已通过语法扫描的文本做替换；检测未闭合/非法片段见
# _scan_placeholders（finditer 会静默忽略未闭合的 {name，不能用于校验）。
PLACEHOLDER_FIND_RE = re.compile(r"\{([a-zA-Z_][a-zA-Z0-9_]*)(?:\|(mask))?\}")
# 完整占位符内部语法（{name} / {name|mask}）；| 后只允许 mask
PLACEHOLDER_INNER_RE = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)(?:\|(mask))?")
# 按对扫描大括号：每个 '{' 必须配对 '}'，否则就是未闭合/孤立片段
BRACE_SCAN_RE = re.compile(r"[{}]")

# ---- 请求模型 ----------------------------------------------------------------


class VariableSpec(BaseModel):
    name: str
    type: str = "string"                    # string|int|number|bool
    required: bool = True
    default: str | int | float | bool | None = None
    sensitive: bool = False
    description: str = ""


class LocalizedText(BaseModel):
    language: str
    subject: str = ""                       # inbox 用作标题；webhook 可空
    body: str


class TemplateDraftRequest(BaseModel):
    event_type: str                         # 事件类型；'*'=通配
    channel: str                            # email|webhook|inbox；'*'=通配通道
    variables: list[VariableSpec] = []
    texts: list[LocalizedText]              # 至少一门语言
    fallback_languages: list[str] = []      # 模板声明的语言回退顺序
    operator: str
    note: str = ""
    upsert: bool = False                    # True：已有草稿时用本次内容整体替换


class TemplateUpdateRequest(BaseModel):
    variables: list[VariableSpec] | None = None
    texts: list[LocalizedText] | None = None
    fallback_languages: list[str] | None = None
    operator: str
    note: str = ""


class PublishRequest(BaseModel):
    operator: str
    reason: str = ""


class PreviewRequest(BaseModel):
    variables: dict = {}
    language: str | None = None             # 缺省=系统缺省语言
    channels: list[str] | None = None       # 缺省=模板通道（'*' 时按三个外发/站内通道）


class RetryRenderRequest(BaseModel):
    operator: str
    variables: dict | None = None           # 覆盖/补齐变量（与事件 payload 合并，审计留痕）
    note: str = ""


# ---- 定义规范化与发布校验 ------------------------------------------------------

class TemplateError(Exception):
    """模板校验/渲染错误（带稳定原因代码，便于落库与查询）。"""

    def __init__(self, code: str, message: str, *, template_version_id: int | None = None,
                 language: str | None = None, language_chain: list[str] | None = None):
        super().__init__(message)
        self.code = code
        self.message = message
        self.template_version_id = template_version_id
        self.language = language
        self.language_chain = language_chain or []


def _norm_variable(spec: dict, index: int) -> dict:
    name = str(spec.get("name") or "").strip()
    if not name or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", name):
        raise HTTPException(422, f"variables[{index}].name must be a non-empty identifier "
                                 f"matching [a-zA-Z_][a-zA-Z0-9_]* (got {spec.get('name')!r})")
    vtype = str(spec.get("type") or "string")
    if vtype not in VARIABLE_TYPES:
        raise HTTPException(422, f"variables[{index}].type must be one of "
                                 f"{','.join(VARIABLE_TYPES)} (got {vtype!r})")
    required = bool(spec.get("required", True))
    sensitive = bool(spec.get("sensitive", False))
    default = spec.get("default")
    if default is not None:
        # 默认值必须与声明类型一致（bool 必须是真布尔，不接受 0/1 字符串混用）
        try:
            default = _coerce(default, vtype)
        except TemplateError as exc:
            raise HTTPException(422, f"variables[{index}].default: {exc.message}")
    if not required and default is None and sensitive:
        # 可选敏感变量无默认值是允许的（缺变量即阻断），这里不做限制
        pass
    return {"name": name, "type": vtype, "required": required,
            "default": default, "sensitive": sensitive,
            "description": str(spec.get("description") or "")}


def _norm_definition(req: dict) -> dict:
    """把 API 提交规范化为存储定义；结构非法直接 422。"""
    event_type = str(req.get("event_type") or "").strip()
    channel = str(req.get("channel") or "").strip()
    if not event_type:
        raise HTTPException(422, "event_type must be non-empty ('*' for wildcard)")
    if channel not in TEMPLATE_CHANNELS:
        raise HTTPException(422, f"channel must be one of {','.join(TEMPLATE_CHANNELS)} "
                                 f"(got {channel!r})")
    variables, seen = [], set()
    for i, v in enumerate(req.get("variables") or []):
        norm = _norm_variable(v if isinstance(v, dict) else v.model_dump(), i)
        if norm["name"] in seen:
            raise HTTPException(422, f"duplicate variable name {norm['name']!r}")
        seen.add(norm["name"])
        variables.append(norm)
    texts_in = req.get("texts") or []
    if not texts_in:
        raise HTTPException(422, "texts must be a non-empty list (at least one language)")
    texts, langs = {}, set()
    for i, t in enumerate(texts_in):
        t = t if isinstance(t, dict) else t.model_dump()
        lang = str(t.get("language") or "").strip()
        if not re.fullmatch(r"[a-zA-Z]{2,3}(-[a-zA-Z0-9]+)*", lang):
            raise HTTPException(422, f"texts[{i}].language must be a language code "
                                     f"like 'zh' or 'en-US' (got {lang!r})")
        if lang in langs:
            raise HTTPException(422, f"duplicate text for language {lang!r}")
        langs.add(lang)
        body = str(t.get("body") or "")
        subject = str(t.get("subject") or "")
        texts[lang] = {"language": lang, "subject": subject, "body": body}
    fallback = []
    for lang in req.get("fallback_languages") or []:
        lang = str(lang).strip()
        if lang and lang not in fallback:
            fallback.append(lang)
    return {"event_type": event_type, "channel": channel, "variables": variables,
            "texts": texts, "languages": sorted(langs), "fallback_languages": fallback}


def _scan_placeholders(text: str) -> list[tuple[str, str | None]]:
    """逐字符扫描大括号：每个 '{' 必须在其后（下一个 '{' 之前）配对 '}'，
    且括号内必须是 {name} / {name|mask}；孤立 '}' 同样拒绝。

    这样未闭合的 '{name'、'{1a}'、'{ a }'、'{name|x}'、孤立 '}' 都不会漏过，
    也不会被替换正则静默忽略后把原文发送出去。
    """
    out: list[tuple[str, str | None]] = []
    pos = 0
    while True:
        m = BRACE_SCAN_RE.search(text, pos)
        if m is None:
            return out
        if m.group() == "}":
            raise TemplateError(REASON_UNCLOSED_PLACEHOLDER,
                                "unmatched '}' without an opening '{'")
        start = m.end()
        close = text.find("}", start)
        reopen = text.find("{", start)
        if close == -1 or (reopen != -1 and reopen < close):
            fragment = text[start: reopen if reopen != -1 else len(text)]
            raise TemplateError(
                REASON_UNCLOSED_PLACEHOLDER,
                f"unclosed placeholder {('{' + fragment)!r}: missing '}}'")
        inner = text[start:close]
        im = PLACEHOLDER_INNER_RE.fullmatch(inner)
        if im is None:
            raise TemplateError(
                REASON_RENDER_ERROR,
                f"invalid placeholder syntax {('{' + inner + '}')!r}: only "
                "{{name}} or {name|mask} are supported")
        out.append((im.group(1), im.group(2)))
        pos = close + 1


def _extract_placeholders(text: str) -> list[tuple[str, str | None]]:
    """提取文本中的占位符（名称, 过滤器）。未闭合的 {、孤立 } 或名称非法即报错。"""
    return _scan_placeholders(text)


def validate_definition(defn: dict) -> list[str]:
    """发布前整份校验，返回警告列表；发现硬错误抛 TemplateError（第一个错误）。

    硬错误：未闭合/非法占位符、未知占位符、未知过滤器、敏感变量未在所有占位符上脱敏、
    语言间占位符不一致、必填变量未被任何文本使用（按需求「校验变量定义与正文占位符」）、
    标题/正文为空。
    """
    variables = {v["name"]: v for v in defn["variables"]}
    warnings: list[str] = []
    base: set[tuple[str, str | None]] | None = None
    base_lang = None
    for lang in defn["languages"]:
        text = defn["texts"][lang]
        if not text["body"].strip():
            raise TemplateError(REASON_RENDER_ERROR,
                                f"language {lang!r}: body must be non-empty")
        if defn["channel"] != notif.CHANNEL_WEBHOOK and not text["subject"].strip():
            raise TemplateError(REASON_RENDER_ERROR,
                                f"language {lang!r}: subject must be non-empty for "
                                f"channel {defn['channel']!r}")
        try:
            placeholders = _extract_placeholders(text["subject"])
            placeholders += _extract_placeholders(text["body"])
        except TemplateError as exc:
            exc.message = f"language {lang!r}: {exc.message}"
            raise
        names = {n for n, _ in placeholders}
        unknown = sorted(names - set(variables))
        if unknown:
            raise TemplateError(REASON_RENDER_ERROR,
                                f"language {lang!r}: placeholder(s) {unknown} have no "
                                "variable declaration")
        # 敏感变量：只要该语言文本中出现它的占位符，就必须带 |mask
        for name, filt in placeholders:
            if variables[name]["sensitive"] and filt != "mask":
                raise TemplateError(
                    REASON_SENSITIVE_UNMASKED,
                    f"language {lang!r}: sensitive variable {name!r} must be rendered "
                    "with the |mask filter (e.g. {" + name + "|mask})")
        sig = set(placeholders)
        if base is None:
            base, base_lang = sig, lang
        elif sig != base:
            raise TemplateError(REASON_RENDER_ERROR,
                                f"placeholders in language {lang!r} differ from "
                                f"{base_lang!r}: all languages must use the same "
                                "placeholder set")
    used = {n for lang in defn["languages"]
            for n, _ in (_extract_placeholders(defn["texts"][lang]["subject"])
                         + _extract_placeholders(defn["texts"][lang]["body"]))}
    for name, v in variables.items():
        if v["required"] and name not in used:
            raise TemplateError(REASON_RENDER_ERROR,
                                f"required variable {name!r} is declared but never used "
                                "in any subject/body")
        if not v["required"] and name not in used:
            warnings.append(f"optional variable {name!r} is declared but never used")
    # 回退顺序中声明了模板不提供的语言：警告（发布仍允许，可能有意跨版本/系统缺省衔接）
    missing_fb = [l for l in defn["fallback_languages"]
                  if l not in defn["languages"]]
    if missing_fb:
        warnings.append("fallback_languages not provided by this template: "
                        + ",".join(missing_fb))
    return warnings


def definition_fingerprint(defn: dict) -> str:
    payload = json.dumps(
        {"event_type": defn["event_type"], "channel": defn["channel"],
         "variables": sorted(defn["variables"], key=lambda v: v["name"]),
         "texts": defn["texts"], "languages": defn["languages"],
         "fallback_languages": defn["fallback_languages"]},
        ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


# ---- 渲染 --------------------------------------------------------------------

def _coerce(value, vtype: str):
    """把变量值按声明类型转换；无法转换抛 type_mismatch。"""
    if value is None:
        raise TemplateError(REASON_TYPE_MISMATCH, "value is null")
    if vtype == "string":
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (str, int, float)):
            return str(value)
        raise TemplateError(REASON_TYPE_MISMATCH,
                            f"expected string, got {type(value).__name__}")
    if vtype == "bool":
        if isinstance(value, bool):
            return value
        if isinstance(value, str) and value.strip().lower() in ("true", "false"):
            return value.strip().lower() == "true"
        raise TemplateError(REASON_TYPE_MISMATCH,
                            f"expected bool (true/false), got {value!r}")
    if vtype == "int":
        if isinstance(value, bool):
            raise TemplateError(REASON_TYPE_MISMATCH, f"expected int, got bool {value!r}")
        if isinstance(value, int):
            return value
        if isinstance(value, str) and re.fullmatch(r"-?\d+", value.strip()):
            return int(value.strip())
        if isinstance(value, float) and value.is_integer():
            return int(value)
        raise TemplateError(REASON_TYPE_MISMATCH, f"expected int, got {value!r}")
    if vtype == "number":
        if isinstance(value, bool):
            raise TemplateError(REASON_TYPE_MISMATCH, f"expected number, got bool {value!r}")
        if isinstance(value, (int, float)):
            return value
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                pass
        raise TemplateError(REASON_TYPE_MISMATCH, f"expected number, got {value!r}")
    raise TemplateError(REASON_TYPE_MISMATCH, f"unknown type {vtype!r}")


def mask_value(value) -> str:
    """敏感变量脱敏：字符串保留末 4 位（不足时全掩）；其余类型统一 ****。"""
    if isinstance(value, str):
        if len(value) <= 4:
            return "****"
        return "****" + value[-4:]
    return "****"


def _format(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def build_language_chain(*, preferred: str | None, fallback_languages: list[str],
                         default_language: str) -> list[str]:
    chain: list[str] = []
    for lang in [preferred, *fallback_languages, default_language]:
        if lang and lang not in chain:
            chain.append(lang)
    return chain


def effective_variables(declarations: list[dict], supplied: dict) -> dict:
    """计算生效变量：supplied 覆盖 -> 默认值；返回 {name: (转换后值, 声明)}。

    缺失必填变量抛 missing_variable；类型转换失败抛 type_mismatch。
    """
    out = {}
    for decl in declarations:
        name = decl["name"]
        present = name in supplied and supplied[name] is not None
        if not present:
            if decl["default"] is not None:
                value = decl["default"]
            elif decl["required"]:
                raise TemplateError(REASON_MISSING_VARIABLE,
                                    f"missing required variable {name!r}")
            else:
                continue  # 可选且无默认：保留未提供，占位符校验在渲染时报缺失
        else:
            value = _coerce(supplied[name], decl["type"])
        out[name] = value
    return out


def _render_text(text: str, values: dict, declarations: dict[str, dict]) -> str:
    # 深度防护：模板正文必须全部是合法且闭合的占位符，未闭合/非法片段直接阻断，
    # 绝不允许被替换正则静默忽略后把形如 {name 的原文发送出去（发布校验已挡，
    # 这里防历史脏数据/绕过发布的版本）。
    _scan_placeholders(text)

    def repl(m: re.Match) -> str:
        name, filt = m.group(1), m.group(2)
        if name not in values:
            # 可选变量无默认且未提供：占位符存在即视为缺失
            raise TemplateError(REASON_MISSING_VARIABLE,
                                f"no value for optional variable {name!r}")
        value = values[name]
        if filt == "mask":
            return mask_value(value)
        return _format(value)

    return PLACEHOLDER_FIND_RE.sub(repl, text)


def _channel_limits(settings: Settings, channel: str) -> tuple[int, int]:
    """返回 (标题上限, 正文上限)。通配模板按最严格口径校验（预览/发布不校验长度，
    长度在按具体通道渲染发送时判定）。"""
    if channel == notif.CHANNEL_EMAIL:
        return settings.notif_email_subject_max, settings.notif_email_body_max
    if channel == notif.CHANNEL_WEBHOOK:
        return settings.notif_webhook_subject_max, settings.notif_webhook_body_max
    return settings.notif_inbox_title_max, settings.notif_inbox_body_max


def render_template(defn: dict, *, channel: str, supplied: dict,
                    language_chain: list[str], settings: Settings,
                    ) -> dict:
    """用已解析的模板定义渲染某一具体通道的最终内容。

    返回 {language, subject, body, variables_snapshot}；失败抛 TemplateError（带原因码）。
    channel 必须是具体通道（email/webhook/inbox），不能是通配。
    """
    # 1) 语言选择：回退链上第一门模板提供且正文非空的语言
    chosen = None
    for lang in language_chain:
        if lang in defn["texts"] and defn["texts"][lang]["body"].strip():
            chosen = lang
            break
    if chosen is None:
        raise TemplateError(REASON_MISSING_LANGUAGE,
                            f"no template body for any language in chain "
                            f"{language_chain} (template provides "
                            f"{defn['languages']})",
            template_version_id=defn["id"],
            language_chain=language_chain)
    declarations = {v["name"]: v for v in defn["variables"]}
    values = effective_variables(defn["variables"], supplied)
    text = defn["texts"][chosen]
    try:
        subject = _render_text(text["subject"], values, declarations)
        body = _render_text(text["body"], values, declarations)
    except TemplateError as exc:
        exc.template_version_id = defn["id"]
        exc.language = chosen
        exc.language_chain = language_chain
        raise
    # 2) 敏感变量深度防护：即便模板漏配 |mask（发布校验已挡），原文也不允许出现在正文
    for name, value in values.items():
        if declarations[name]["sensitive"]:
            rendered_token = _format(value)
            if rendered_token and rendered_token in (subject + body):
                raise TemplateError(
                    REASON_SENSITIVE_UNMASKED,
                    f"sensitive variable {name!r} raw value appears in rendered "
                    "content without |mask",
                    template_version_id=defn["id"], language=chosen,
                    language_chain=language_chain)
    # 3) 通道长度上限
    subj_max, body_max = _channel_limits(settings, channel)
    if len(body) > body_max:
        raise TemplateError(REASON_BODY_TOO_LONG,
                            f"rendered body length {len(body)} exceeds channel "
                            f"{channel} limit {body_max}",
                            template_version_id=defn["id"], language=chosen,
                            language_chain=language_chain)
    if channel != notif.CHANNEL_WEBHOOK and len(subject) > subj_max:
        raise TemplateError(REASON_SUBJECT_TOO_LONG,
                            f"rendered subject length {len(subject)} exceeds channel "
                            f"{channel} limit {subj_max}",
                            template_version_id=defn["id"], language=chosen,
                            language_chain=language_chain)
    # 变量快照：敏感变量只固化脱敏值（快照随发送任务永久保存，不能落原文）
    snapshot = {}
    for name, value in values.items():
        if declarations[name]["sensitive"]:
            snapshot[name] = mask_value(value)
        else:
            snapshot[name] = value
    return {"language": chosen, "language_chain": language_chain,
            "subject": subject, "body": body, "variables_snapshot": snapshot}


def content_hash(*, channel: str, subject: str, body: str) -> str:
    return hashlib.sha256(
        json.dumps({"channel": channel, "subject": subject, "body": body},
                   ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()


# ---- 库里的版本存取 ------------------------------------------------------------

def _row_to_defn(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"], "event_type": row["event_type"], "channel": row["channel"],
        "version": row["version"], "status": row["status"],
        "variables": json.loads(row["variables_json"]),
        "subjects": json.loads(row["subjects_json"]),
        "bodies": json.loads(row["bodies_json"]),
        "languages": json.loads(row["languages_json"]),
        "fallback_languages": json.loads(row["fallback_languages_json"]),
        "texts": {lang: {"language": lang,
                         "subject": json.loads(row["subjects_json"]).get(lang, ""),
                         "body": json.loads(row["bodies_json"]).get(lang, "")}
                  for lang in json.loads(row["languages_json"])},
        "content_sha256": row["content_sha256"],
        "operator": row["operator"], "note": row["note"],
        "created_at": row["created_at"], "published_at": row["published_at"]}


def resolve_template_tx(cur: sqlite3.Cursor, event_type: str,
                        channel: str) -> sqlite3.Row | None:
    """按 精确 -> 事件通配 -> 通道通配 -> 双通配 顺序解析当前生效模板版本行。"""
    for et, ch in ((event_type, channel), (event_type, WILDCARD),
                   (WILDCARD, channel), (WILDCARD, WILDCARD)):
        cur_row = cur.execute(
            "SELECT tv.* FROM notif_template_current tc "
            "JOIN notif_template_versions tv ON tv.id = tc.template_version_id "
            "WHERE tc.event_type=? AND tc.channel=?", (et, ch)).fetchone()
        if cur_row is not None:
            return cur_row
    return None


def recipient_language(cur: sqlite3.Cursor, recipient: str,
                       settings: Settings) -> str:
    row = cur.execute("SELECT language FROM approval_contacts WHERE name=?",
                      (recipient,)).fetchone()
    lang = row["language"] if row is not None and row["language"] else None
    return lang or settings.notif_default_language


def render_for_channel_tx(cur: sqlite3.Cursor, *, event_type: str, channel: str,
                          recipient: str, supplied: dict, settings: Settings,
                          ) -> dict:
    """入队事务内：解析当前模板并渲染某通道最终内容。

    返回 {"templated": True, "template_version_id", "template": defn, "rendered": {...}}
    或 {"templated": False}（该键未配置模板，调用方沿用事件静态正文）。
    渲染失败抛 TemplateError（带原因码/语言链/模板版本，调用方负责落失败记录）。
    """
    row = resolve_template_tx(cur, event_type, channel)
    if row is None:
        return {"templated": False}
    defn = _row_to_defn(row)
    preferred = recipient_language(cur, recipient, settings)
    chain = build_language_chain(
        preferred=preferred, fallback_languages=defn["fallback_languages"],
        default_language=settings.notif_default_language)
    rendered = render_template(defn, channel=channel, supplied=supplied,
                               language_chain=chain, settings=settings)
    return {"templated": True, "template_version_id": row["id"],
            "template": defn, "rendered": rendered}


# ---- 失败记录 ------------------------------------------------------------------

def render_event_channels_tx(cur: sqlite3.Cursor, *, event_type: str, recipient: str,
                             channels, supplied: dict, settings: Settings,
                             now: float, entity_type: str,
                             todo_id: int | None, event_id: int,
                             send_task_id: int | None = None,
                             delivery_id: int | None = None) -> dict:
    """为指定通道集合（联系人启用通道或路由计划通道）逐个解析并渲染模板。

    返回 {channel: {"outcome": "static"|"rendered"|"failed", ...}}：
    - static：该通道没有配置模板（调用方沿用事件静态正文）；
    - rendered：带 template_version_id 与 rendered 结果；
    - failed：渲染失败，已落 notif_template_render_failures，带 code/message。
    重复渲染（未解除失败已存在）只更新同一行，绝不产生第二份记录/发送效果。
    """
    result = {}
    for ch in channels:
        try:
            rendered = render_for_channel_tx(
                cur, event_type=event_type, channel=ch, recipient=recipient,
                supplied=supplied, settings=settings)
        except TemplateError as exc:
            record_failure_tx(
                cur, entity_type=entity_type, channel=ch, todo_id=todo_id,
                event_id=event_id, recipient=recipient, event_type=event_type,
                exc=exc, send_task_id=send_task_id, delivery_id=delivery_id,
                now=now)
            result[ch] = {"outcome": "failed", "code": exc.code,
                          "message": exc.message,
                          "template_version_id": exc.template_version_id}
            continue
        if not rendered["templated"]:
            result[ch] = {"outcome": "static"}
        else:
            result[ch] = {"outcome": "rendered",
                          "template_version_id": rendered["template_version_id"],
                          "rendered": rendered["rendered"]}
    return result


def record_failure_tx(cur: sqlite3.Cursor, *, entity_type: str, channel: str,
                      todo_id: int | None, event_id: int, recipient: str,
                      event_type: str, exc: TemplateError,
                      send_task_id: int | None = None,
                      delivery_id: int | None = None,
                      variables_snapshot: dict | None = None,
                      now: float) -> int:
    """登记一条渲染失败（未解除唯一索引兜底重复渲染不产生第二条）。已存在未解除
    记录时更新原因与时间（返回既有 id），全程不产生外发效果。"""
    existing = cur.execute(
        "SELECT id FROM notif_template_render_failures WHERE resolved_at IS NULL "
        "AND channel=? AND "
        "((? IS NOT NULL AND send_task_id=?) "
        "OR (? IS NOT NULL AND delivery_id=?))",
        (channel, send_task_id, send_task_id, delivery_id, delivery_id)).fetchone()
    if existing is not None:
        cur.execute(
            "UPDATE notif_template_render_failures SET reason_code=?, reason_detail=?, "
            "template_version_id=?, language=?, language_chain_json=?, "
            "variables_snapshot_json=?, updated_at=? WHERE id=?",
            (exc.code, exc.message, exc.template_version_id, exc.language,
             json.dumps(exc.language_chain, ensure_ascii=False),
             json.dumps(variables_snapshot or {}, ensure_ascii=False, sort_keys=True),
             now, existing["id"]))
        return int(existing["id"])
    cur.execute(
        """INSERT INTO notif_template_render_failures
           (entity_type, send_task_id, delivery_id, todo_id, event_id, recipient,
            channel, event_type, template_version_id, language, language_chain_json,
            reason_code, reason_detail, variables_snapshot_json, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (entity_type, send_task_id, delivery_id, todo_id, event_id, recipient,
         channel, event_type, exc.template_version_id, exc.language,
         json.dumps(exc.language_chain, ensure_ascii=False),
         exc.code, exc.message,
         json.dumps(variables_snapshot or {}, ensure_ascii=False, sort_keys=True),
         now, now))
    return int(cur.lastrowid)


def resolve_open_failures_tx(cur: sqlite3.Cursor, *, send_task_id: int | None = None,
                             delivery_id: int | None = None, operator: str,
                             note: str, now: float) -> int:
    where, params = [], []
    if send_task_id is not None:
        where.append("send_task_id=?")
        params.append(send_task_id)
    if delivery_id is not None:
        where.append("delivery_id=?")
        params.append(delivery_id)
    if not where:
        return 0
    cur.execute(
        "UPDATE notif_template_render_failures SET resolved_at=?, resolved_by=?, "
        "resolve_note=?, updated_at=? WHERE resolved_at IS NULL AND ("
        + " OR ".join(where) + ")",
        (now, operator, note, now, *params))
    return cur.rowcount


def open_failure_exists_tx(cur: sqlite3.Cursor, *, send_task_id: int | None = None,
                           delivery_id: int | None = None) -> bool:
    row = cur.execute(
        "SELECT 1 AS x FROM notif_template_render_failures WHERE resolved_at IS NULL "
        "AND channel IS NOT NULL AND "
        "((? IS NOT NULL AND send_task_id=?) "
        "OR (? IS NOT NULL AND delivery_id=?)) LIMIT 1",
        (send_task_id, send_task_id, delivery_id, delivery_id)).fetchone()
    return row is not None


# ---- 草稿 / 发布 / 预览 ---------------------------------------------------------

def create_or_get_draft(db: Database, req: TemplateDraftRequest, now: float | None = None) -> dict:
    operator = notif._require(req.operator, "operator")
    defn = _norm_definition(req.model_dump())
    now = time.time() if now is None else now
    with db.tx() as cur:
        existing = cur.execute(
            "SELECT * FROM notif_template_versions WHERE event_type=? AND channel=? "
            "AND status='draft'", (defn["event_type"], defn["channel"])).fetchone()
        if existing is not None and not req.upsert:
            # 重复创建不产生第二份草稿：幂等返回既有草稿（已发布过的键也仍是同一份草稿）
            return {"draft": _draft_view(existing), "created": False}
        if existing is not None and req.upsert:
            # 整体替换既有草稿（草稿可反复编辑，不产生新版本行）
            cur.execute(
                """UPDATE notif_template_versions SET variables_json=?, subjects_json=?,
                   bodies_json=?, languages_json=?, fallback_languages_json=?,
                   content_sha256=NULL, operator=?, note=?, created_at=? WHERE id=?""",
                (json.dumps(defn["variables"], ensure_ascii=False),
                 json.dumps({l: defn["texts"][l]["subject"] for l in defn["languages"]},
                            ensure_ascii=False, sort_keys=True),
                 json.dumps({l: defn["texts"][l]["body"] for l in defn["languages"]},
                            ensure_ascii=False, sort_keys=True),
                 json.dumps(defn["languages"], ensure_ascii=False),
                 json.dumps(defn["fallback_languages"], ensure_ascii=False),
                 operator, req.note or None, now, existing["id"]))
            audit.record(cur, "notif_template_draft_updated", None, None, {
                "template_version_id": existing["id"],
                "event_type": defn["event_type"], "channel": defn["channel"],
                "operator": operator, "languages": defn["languages"]}, ts=now)
            return {"draft": _draft_view(
                cur.execute("SELECT * FROM notif_template_versions WHERE id=?",
                            (existing["id"],)).fetchone()), "created": False}
        cur.execute(
            """INSERT INTO notif_template_versions
               (event_type, channel, version, status, variables_json, subjects_json,
                bodies_json, languages_json, fallback_languages_json, content_sha256,
                operator, note, created_at)
               VALUES (?,?,NULL,'draft',?,?,?,?,?,NULL,?,?,?)""",
            (defn["event_type"], defn["channel"],
             json.dumps(defn["variables"], ensure_ascii=False),
             json.dumps({l: defn["texts"][l]["subject"] for l in defn["languages"]},
                        ensure_ascii=False, sort_keys=True),
             json.dumps({l: defn["texts"][l]["body"] for l in defn["languages"]},
                        ensure_ascii=False, sort_keys=True),
             json.dumps(defn["languages"], ensure_ascii=False),
             json.dumps(defn["fallback_languages"], ensure_ascii=False),
             operator, req.note or None, now))
        draft_id = cur.lastrowid
        audit.record(cur, "notif_template_draft_created", None, None, {
            "template_version_id": draft_id, "event_type": defn["event_type"],
            "channel": defn["channel"], "languages": defn["languages"],
            "variables": [v["name"] for v in defn["variables"]],
            "operator": operator}, ts=now)
        return {"draft": _draft_view(
            cur.execute("SELECT * FROM notif_template_versions WHERE id=?",
                        (draft_id,)).fetchone()), "created": True}


def update_draft(db: Database, event_type: str, channel: str,
                 req: TemplateUpdateRequest, now: float | None = None) -> dict:
    operator = notif._require(req.operator, "operator")
    now = time.time() if now is None else now
    with db.tx() as cur:
        row = cur.execute(
            "SELECT * FROM notif_template_versions WHERE event_type=? AND channel=? "
            "AND status='draft'", (event_type, channel)).fetchone()
        if row is None:
            raise HTTPException(404, f"no draft for ({event_type!r}, {channel!r}); "
                                     "create it first")
        current = _row_to_defn(row)
        merged = {"event_type": event_type, "channel": channel,
                  "variables": (req.variables if req.variables is not None
                                else current["variables"]),
                  "texts": (req.texts if req.texts is not None
                            else list(current["texts"].values())),
                  "languages": None,
                  "fallback_languages": (req.fallback_languages
                                         if req.fallback_languages is not None
                                         else current["fallback_languages"])}
        if req.variables is not None:
            merged["variables"] = [v if isinstance(v, dict) else v.model_dump()
                                   for v in merged["variables"]]
        defn = _norm_definition(merged)
        fp = definition_fingerprint(defn)
        cur.execute(
            """UPDATE notif_template_versions SET variables_json=?, subjects_json=?,
               bodies_json=?, languages_json=?, fallback_languages_json=?,
               content_sha256=?, operator=?, note=?, created_at=? WHERE id=?""",
            (json.dumps(defn["variables"], ensure_ascii=False),
             json.dumps({l: defn["texts"][l]["subject"] for l in defn["languages"]},
                        ensure_ascii=False, sort_keys=True),
             json.dumps({l: defn["texts"][l]["body"] for l in defn["languages"]},
                        ensure_ascii=False, sort_keys=True),
             json.dumps(defn["languages"], ensure_ascii=False),
             json.dumps(defn["fallback_languages"], ensure_ascii=False),
             fp, operator, req.note or None, now, row["id"]))
        audit.record(cur, "notif_template_draft_updated", None, None, {
            "template_version_id": row["id"], "event_type": event_type,
            "channel": channel, "operator": operator,
            "languages": defn["languages"]}, ts=now)
        return {"draft": _draft_view(
            cur.execute("SELECT * FROM notif_template_versions WHERE id=?",
                        (row["id"],)).fetchone())}


def validate_draft(db: Database, event_type: str, channel: str) -> dict:
    """只校验不发布：返回错误列表与警告列表（HTTP 始终 200，便于运营在界面逐条修）。"""
    with db.tx() as cur:
        row = _draft_or_404(cur, event_type, channel)
        defn = _row_to_defn(row)
        errors, warnings = _safe_validate(defn)
        return {"event_type": event_type, "channel": channel,
                "draft_id": row["id"], "valid": not errors,
                "errors": errors, "warnings": warnings}


def _safe_validate(defn: dict) -> tuple[list[dict], list[str]]:
    try:
        warnings = validate_definition(defn)
    except TemplateError as exc:
        return [{"code": exc.code, "message": exc.message}], []
    return [], warnings


def preview_draft(db: Database, event_type: str, channel: str,
                  req: PreviewRequest, settings: Settings) -> dict:
    """用给定变量只读渲染草稿：按通道返回采用语言、最终标题/正文与错误；不发送、不留失败。"""
    with db.tx() as cur:
        row = _draft_or_404(cur, event_type, channel)
        return _preview(cur, _row_to_defn(row), req, settings)


def preview_version(db: Database, version_id: int, req: PreviewRequest,
                    settings: Settings) -> dict:
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM notif_template_versions WHERE id=?",
                          (version_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "template version not found")
        return _preview(cur, _row_to_defn(row), req, settings)


def _preview(cur: sqlite3.Cursor, defn: dict, req: PreviewRequest,
             settings: Settings) -> dict:
    errors, warnings = _safe_validate(defn)
    channels = req.channels or (
        [defn["channel"]] if defn["channel"] != WILDCARD
        else [notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK, CHANNEL_INBOX])
    previews = []
    chain = build_language_chain(
        preferred=req.language or settings.notif_default_language,
        fallback_languages=defn["fallback_languages"],
        default_language=settings.notif_default_language)
    for ch in channels:
        if ch not in (notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK,
                      CHANNEL_INBOX):
            raise HTTPException(422, f"unsupported preview channel {ch!r}")
        item = {"channel": ch}
        try:
            rendered = render_template(defn, channel=ch, supplied=req.variables,
                                       language_chain=chain, settings=settings)
            item.update(ok=True, language=rendered["language"],
                        language_chain=rendered["language_chain"],
                        subject=rendered["subject"], body=rendered["body"],
                        variables_snapshot=rendered["variables_snapshot"],
                        content_sha256=content_hash(
                            channel=ch, subject=rendered["subject"],
                            body=rendered["body"]))
        except TemplateError as exc:
            item.update(ok=False, error={"code": exc.code, "message": exc.message},
                        language=exc.language, language_chain=exc.language_chain)
        previews.append(item)
    return {"event_type": defn["event_type"], "channel": defn["channel"],
            "version_id": defn.get("id"), "status": defn.get("status"),
            "requested_language": req.language, "language_chain": chain,
            "languages": defn["languages"], "errors": errors, "warnings": warnings,
            "previews": previews}


def publish_draft(db: Database, event_type: str, channel: str,
                  req: PublishRequest, now: float | None = None) -> dict:
    """发布草稿：整份校验通过才生成不可变版本并推进当前指针（同一事务）。

    校验失败落 rejected 记录（原因可查），草稿保留、指针不动；重复发布相同定义幂等
    返回当前版本，不产生第二个版本。
    """
    operator = notif._require(req.operator, "operator")
    now = time.time() if now is None else now
    with db.tx() as cur:
        row = _draft_or_404(cur, event_type, channel)
        defn = _row_to_defn(row)
    # 校验失败：在独立事务落 rejected 记录与审计（HTTPException 会回滚其所在事务，
    # 因此被拒绝的提交必须先单独提交），随后抛 422，当前指针与草稿都不变。
    try:
        warnings = validate_definition(defn)
    except TemplateError as exc:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO notif_template_versions
                   (event_type, channel, version, status, variables_json, subjects_json,
                    bodies_json, languages_json, fallback_languages_json, operator,
                    note, rejection_reason, created_at)
                   VALUES (?,?,NULL,'rejected',?,?,?,?,?,?,?,?,?)""",
                (event_type, channel,
                 json.dumps(defn["variables"], ensure_ascii=False),
                 json.dumps(defn["subjects"], ensure_ascii=False, sort_keys=True),
                 json.dumps(defn["bodies"], ensure_ascii=False, sort_keys=True),
                 json.dumps(defn["languages"], ensure_ascii=False),
                 json.dumps(defn["fallback_languages"], ensure_ascii=False),
                 operator, req.reason or None,
                 json.dumps([{"code": exc.code, "message": exc.message}],
                            ensure_ascii=False), now))
            rejected_id = cur.lastrowid
            audit.record(cur, "notif_template_publish_rejected", None, None, {
                "template_version_id": rejected_id, "event_type": event_type,
                "channel": channel, "operator": operator,
                "error": {"code": exc.code, "message": exc.message}}, ts=now)
        raise HTTPException(422, [{"code": exc.code, "message": exc.message}])
    fp = definition_fingerprint(defn)
    with db.tx() as cur:
        # 重复发布：当前指针已指向内容指纹相同的版本 -> 幂等返回，不生成新版本
        pointer = cur.execute(
            "SELECT * FROM notif_template_current WHERE event_type=? AND channel=?",
            (event_type, channel)).fetchone()
        if pointer is not None:
            cur_row = cur.execute(
                "SELECT * FROM notif_template_versions WHERE id=?",
                (pointer["template_version_id"],)).fetchone()
            if cur_row["content_sha256"] == fp:
                audit.record(cur, "notif_template_publish_duplicate", None, None, {
                    "event_type": event_type, "channel": channel,
                    "version": pointer["version"], "operator": operator}, ts=now)
                return {"result": "unchanged", "version": pointer["version"],
                        "template_version_id": pointer["template_version_id"],
                        "warnings": warnings}
        vrow = cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM notif_template_versions "
            "WHERE event_type=? AND channel=? AND status='published'",
            (event_type, channel)).fetchone()
        version = int(vrow["v"])
        cur.execute(
            """INSERT INTO notif_template_versions
               (event_type, channel, version, status, variables_json, subjects_json,
                bodies_json, languages_json, fallback_languages_json, content_sha256,
                operator, note, created_at, published_at)
               VALUES (?,?,?,'published',?,?,?,?,?,?,?,?,?,?)""",
            (event_type, channel, version,
             json.dumps(defn["variables"], ensure_ascii=False),
             json.dumps(defn["subjects"], ensure_ascii=False, sort_keys=True),
             json.dumps(defn["bodies"], ensure_ascii=False, sort_keys=True),
             json.dumps(defn["languages"], ensure_ascii=False),
             json.dumps(defn["fallback_languages"], ensure_ascii=False),
             fp, operator, req.reason or None, now, now))
        version_id = cur.lastrowid
        prev = cur.execute(
            "SELECT version FROM notif_template_current WHERE event_type=? AND channel=?",
            (event_type, channel)).fetchone()
        cur.execute(
            """INSERT INTO notif_template_current
               (event_type, channel, template_version_id, version, updated_by,
                updated_at, reason)
               VALUES (?,?,?,?,?,?,?)
               ON CONFLICT(event_type, channel) DO UPDATE SET
                 template_version_id=excluded.template_version_id,
                 version=excluded.version, updated_by=excluded.updated_by,
                 updated_at=excluded.updated_at, reason=excluded.reason""",
            (event_type, channel, version_id, version, operator, now,
             req.reason or None))
        audit.record(cur, "notif_template_published", None, None, {
            "event_type": event_type, "channel": channel, "version": version,
            "template_version_id": version_id, "operator": operator,
            "prev_version": prev["version"] if prev else None,
            "languages": defn["languages"],
            "variables": [v["name"] for v in defn["variables"]],
            "warnings": warnings, "content_sha256": fp}, ts=now)
        return {"result": "published", "version": version,
                "template_version_id": version_id, "warnings": warnings}


def _draft_or_404(cur: sqlite3.Cursor, event_type: str, channel: str) -> sqlite3.Row:
    row = cur.execute(
        "SELECT * FROM notif_template_versions WHERE event_type=? AND channel=? "
        "AND status='draft'", (event_type, channel)).fetchone()
    if row is None:
        raise HTTPException(404, f"no draft for (event_type={event_type!r}, "
                                 f"channel={channel!r})")
    return row


# ---- 查询视图 ------------------------------------------------------------------

def _draft_view(row: sqlite3.Row) -> dict:
    return {"id": row["id"], "event_type": row["event_type"],
            "channel": row["channel"], "status": row["status"],
            "variables": json.loads(row["variables_json"]),
            "texts": [{"language": l,
                       "subject": json.loads(row["subjects_json"]).get(l, ""),
                       "body": json.loads(row["bodies_json"]).get(l, "")}
                      for l in json.loads(row["languages_json"])],
            "languages": json.loads(row["languages_json"]),
            "fallback_languages": json.loads(row["fallback_languages_json"]),
            "operator": row["operator"], "note": row["note"],
            "created_at": row["created_at"]}


def _version_view(row: sqlite3.Row, *, with_content: bool = False) -> dict:
    out = {"id": row["id"], "event_type": row["event_type"],
           "channel": row["channel"], "version": row["version"],
           "status": row["status"], "languages": json.loads(row["languages_json"]),
           "fallback_languages": json.loads(row["fallback_languages_json"]),
           "operator": row["operator"], "note": row["note"],
           "rejection_reason": (json.loads(row["rejection_reason"])
                                if row["rejection_reason"] else None),
           "content_sha256": row["content_sha256"],
           "created_at": row["created_at"], "published_at": row["published_at"]}
    if with_content:
        out["variables"] = json.loads(row["variables_json"])
        out["texts"] = [{"language": l,
                         "subject": json.loads(row["subjects_json"]).get(l, ""),
                         "body": json.loads(row["bodies_json"]).get(l, "")}
                        for l in json.loads(row["languages_json"])]
    return out


def list_versions(db: Database, *, event_type: str | None = None,
                  channel: str | None = None, status: str | None = None,
                  limit: int = 100) -> dict:
    sql, params = "SELECT * FROM notif_template_versions WHERE 1=1", []
    if event_type:
        sql += " AND event_type=?"
        params.append(event_type)
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    if status:
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"versions": [_version_view(r) for r in rows], "count": len(rows)}


def get_version(db: Database, version_id: int) -> dict:
    row = db.query_one("SELECT * FROM notif_template_versions WHERE id=?",
                       (version_id,))
    if row is None:
        raise HTTPException(404, "template version not found")
    return {"version": _version_view(row, with_content=True)}


def list_current(db: Database) -> dict:
    rows = db.query(
        """SELECT tc.*, tv.languages_json AS languages_json,
                  tv.content_sha256 AS content_sha256
           FROM notif_template_current tc
           JOIN notif_template_versions tv ON tv.id = tc.template_version_id
           ORDER BY tc.event_type, tc.channel""")
    return {"current": [{
        "event_type": r["event_type"], "channel": r["channel"],
        "version": r["version"], "template_version_id": r["template_version_id"],
        "languages": json.loads(r["languages_json"]),
        "content_sha256": r["content_sha256"], "updated_by": r["updated_by"],
        "updated_at": r["updated_at"], "reason": r["reason"]} for r in rows]}


def list_failures(db: Database, *, resolved: bool | None = False,
                  recipient: str | None = None, event_type: str | None = None,
                  channel: str | None = None, reason_code: str | None = None,
                  send_task_id: int | None = None, limit: int = 100) -> dict:
    sql, params = "SELECT * FROM notif_template_render_failures WHERE 1=1", []
    if resolved is not None:
        sql += " AND resolved_at IS " + ("NOT NULL" if resolved else "NULL")
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if event_type:
        sql += " AND event_type=?"
        params.append(event_type)
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    if reason_code:
        sql += " AND reason_code=?"
        params.append(reason_code)
    if send_task_id is not None:
        sql += " AND send_task_id=?"
        params.append(send_task_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"failures": [_failure_view(r) for r in rows], "count": len(rows)}


def _failure_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "entity_type": r["entity_type"],
            "send_task_id": r["send_task_id"], "delivery_id": r["delivery_id"],
            "todo_id": r["todo_id"], "event_id": r["event_id"],
            "recipient": r["recipient"], "channel": r["channel"],
            "event_type": r["event_type"],
            "template_version_id": r["template_version_id"],
            "language": r["language"],
            "language_chain": json.loads(r["language_chain_json"]),
            "reason_code": r["reason_code"], "reason_detail": r["reason_detail"],
            "variables_snapshot": json.loads(r["variables_snapshot_json"]),
            "resolved": r["resolved_at"] is not None,
            "resolved_by": r["resolved_by"], "resolved_at": r["resolved_at"],
            "resolve_note": r["resolve_note"],
            "created_at": r["created_at"], "updated_at": r["updated_at"]}


# ---- 渲染失败重试（管理员修复模板/变量后） ---------------------------------------

def retry_task_render(db: Database, task_id: int, req: RetryRenderRequest,
                      settings: Settings, now: float | None = None) -> dict:
    """对渲染失败的发送任务按当前模板重新渲染并重建计划快照。

    只在任务仍为 render_failed 时生效；渲染成功 -> 任务回 pending（worker 下一轮按
    新快照派发，失败记录标记 resolved）；仍失败 -> 更新未解除失败记录，任务不动。
    条件状态转移 + 未解除失败唯一索引保证重复点击/并发不产生第二份发送效果。
    """
    operator = notif._require(req.operator, "operator")
    now = time.time() if now is None else now
    with db.tx() as cur:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (task_id,)).fetchone()
        if task is None:
            raise HTTPException(404, "notification send task not found")
        if task["status"] != TASK_RENDER_FAILED:
            raise HTTPException(409, f"send task is {task['status']}, only "
                                     f"{TASK_RENDER_FAILED} tasks can retry rendering")
        supplied = _task_variables(cur, task, req.variables)
        rebuilt, failures = _rebuild_task_plan_tx(
            cur, task, supplied, settings, now, operator=operator)
        if failures:
            # 仍有失败：保持 render_failed 终态（不派发），刷新失败原因与失败记录
            cur.execute(
                """UPDATE notif_send_tasks SET render_failure_reason=?, updated_at=?
                   WHERE id=?""",
                (json.dumps(failures, ensure_ascii=False), now, task_id))
            audit.record(cur, "notif_template_retry_render_failed", None, None, {
                "send_task_id": task_id, "operator": operator,
                "failures": failures, "override_variables":
                    sorted((req.variables or {}).keys())}, ts=now)
            return {"result": "failed", "task_id": task_id,
                    "status": TASK_RENDER_FAILED, "failures": failures}
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', render_status='rendered',
               render_failure_reason=NULL, plan_json=?, content_snapshot=?,
               template_snapshot=?, current_channel=NULL, attempt_index=0,
               total_attempts=0, next_retry_at=NULL, updated_at=? WHERE id=?
               AND status='render_failed'""",
            (json.dumps(rebuilt["plan"], ensure_ascii=False, sort_keys=True),
             json.dumps(rebuilt["content_snapshot"], ensure_ascii=False,
                        sort_keys=True),
             json.dumps(rebuilt["template_snapshot"], ensure_ascii=False,
                        sort_keys=True), now, task_id))
        resolved = resolve_open_failures_tx(
            cur, send_task_id=task_id, operator=operator,
            note=req.note or "retry render succeeded", now=now)
        _routing()._record_switch_tx(
            cur, task_id, task["event_id"], task["recipient"], None,
            rebuilt["plan"][0]["channel"] if rebuilt["plan"] else None,
            _routing().SW_SELECTED,
            {"reason": "render_retry", "plan": [p["channel"] for p in rebuilt["plan"]]},
            now)
        audit.record(cur, "notif_template_retry_render_succeeded", None, None, {
            "send_task_id": task_id, "operator": operator,
            "languages": {p["channel"]: p["language"] for p in rebuilt["plan"]},
            "resolved_failures": resolved,
            "override_variables": sorted((req.variables or {}).keys())}, ts=now)
        return {"result": "rebuilt", "task_id": task_id, "status": "pending",
                "plan": [p["channel"] for p in rebuilt["plan"]],
                "resolved_failures": resolved}


def _task_variables(cur: sqlite3.Cursor, task: sqlite3.Row,
                    overrides: dict | None) -> dict:
    event = cur.execute("SELECT payload FROM approval_notify_events WHERE id=?",
                        (task["event_id"],)).fetchone()
    supplied = json.loads(event["payload"] or "{}") if event else {}
    if overrides:
        supplied.update(overrides)
    return supplied


def _rebuild_task_plan_tx(cur, task, supplied, settings, now, *, operator=None):
    """按当前模板为任务的通道计划重建每通道渲染快照。

    返回 (rebuilt, failures)；failures 非空表示仍有通道渲染失败（已登记/更新失败记录）。
    """
    old_plan = json.loads(task["plan_json"] or "[]")
    channels = [p["channel"] for p in old_plan] or [notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK, CHANNEL_INBOX]
    plan, content_snapshot, failures = [], {}, []
    template_meta = None
    contact = cur.execute(
        "SELECT email, webhook_url FROM approval_contacts WHERE name=? AND active=1",
        (task["recipient"],)).fetchone()
    for ch in channels:
        old_item = next((p for p in old_plan if p["channel"] == ch), {})
        address = old_item.get("address")
        if not address and ch in (notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK):
            if contact is not None:
                address = contact["email"] if ch == notif.CHANNEL_EMAIL \
                    else contact["webhook_url"]
        if ch in (notif.CHANNEL_EMAIL, notif.CHANNEL_WEBHOOK) and not address:
            # 仍无地址：不恢复该通道（与首次入队同口径，记 no_address 切换）
            _routing()._record_switch_tx(
                cur, task["id"], task["event_id"], task["recipient"], None, ch,
                _routing().SW_NO_ADDRESS, {"note": "retry render: no address"}, now)
            continue
        try:
            result = render_for_channel_tx(
                cur, event_type=task["event_type"], channel=ch,
                recipient=task["recipient"], supplied=supplied, settings=settings)
        except TemplateError as exc:
            fid = record_failure_tx(
                cur, entity_type="task", channel=ch, todo_id=task["todo_id"],
                event_id=task["event_id"], recipient=task["recipient"],
                event_type=task["event_type"], exc=exc, send_task_id=task["id"],
                now=now)
            failures.append({"channel": ch, "failure_id": fid,
                             "code": exc.code, "message": exc.message})
            continue
        if not result["templated"]:
            # 模板已被删除/回退到无模板：回退事件静态正文（仍算重建成功）
            rendered = {"language": None, "language_chain": [],
                        "subject": task["subject"], "body": task["body"],
                        "variables_snapshot": {}}
            version_id = None
        else:
            rendered = result["rendered"]
            version_id = result["template_version_id"]
            template_meta = {
                "template_version_id": version_id,
                "variables": result["template"]["variables"],
                "languages": result["template"]["languages"],
                "fallback_languages": result["template"]["fallback_languages"]}
        sha = content_hash(channel=ch, subject=rendered["subject"],
                           body=rendered["body"])
        item = {"channel": ch, "address": address,
                "timeout_seconds": old_item.get("timeout_seconds"),
                "max_attempts": old_item.get("max_attempts"),
                "condition": old_item.get("condition"),
                "subject": rendered["subject"], "body": rendered["body"],
                "template_version_id": version_id,
                "language": rendered["language"],
                "language_chain": rendered["language_chain"],
                "content_sha256": sha}
        if ch == notif.CHANNEL_WEBHOOK:
            item["webhook_payload"] = {
                **supplied, "event": task["event_type"],
                "subject": rendered["subject"], "body": rendered["body"]}
        plan.append(item)
        content_snapshot[ch] = {
            "template_version_id": version_id, "language": rendered["language"],
            "language_chain": rendered["language_chain"],
            "subject": rendered["subject"], "body": rendered["body"],
            "variables_snapshot": rendered["variables_snapshot"],
            "content_sha256": sha}
    rebuilt = {"plan": plan, "content_snapshot": content_snapshot,
               "template_snapshot": template_meta}
    return rebuilt, failures


# ---- 路由 --------------------------------------------------------------------

def create_templates_router(db: Database, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin/approval-notifications/templates",
                       tags=["approval-notification-templates"])

    @router.post("/drafts")
    def create_draft(req: TemplateDraftRequest):
        """为「事件类型×通道」创建草稿（已存在草稿时幂等返回，不产生第二份）。"""
        return create_or_get_draft(db, req)

    @router.put("/drafts/{event_type}/{channel}")
    def update_draft_endpoint(event_type: str, channel: str,
                              req: TemplateUpdateRequest):
        """编辑草稿（仅 draft 可编辑；已发布版本不可变）。"""
        return update_draft(db, event_type, channel, req)

    @router.get("/drafts")
    def drafts(event_type: str | None = None, channel: str | None = None):
        return list_versions(db, event_type=event_type, channel=channel,
                             status=STATUS_DRAFT, limit=1000)

    @router.get("/drafts/{event_type}/{channel}")
    def get_draft(event_type: str, channel: str):
        with db.tx() as cur:
            return {"draft": _draft_view(_draft_or_404(cur, event_type, channel))}

    @router.post("/drafts/{event_type}/{channel}/validate")
    def validate(event_type: str, channel: str):
        """发布前校验（只返回错误/警告，不改状态）。"""
        return validate_draft(db, event_type, channel)

    @router.post("/drafts/{event_type}/{channel}/preview")
    def preview(event_type: str, channel: str, req: PreviewRequest):
        """用给定变量渲染预览（不发送、不留失败记录）。"""
        return preview_draft(db, event_type, channel, req, settings)

    @router.post("/drafts/{event_type}/{channel}/publish")
    def publish(event_type: str, channel: str, req: PublishRequest):
        """发布为不可变版本并推进当前指针；校验失败落 rejected，指针不动。"""
        return publish_draft(db, event_type, channel, req)

    @router.get("/current")
    def current():
        """当前生效模板指针一览（含语言与内容指纹）。"""
        return list_current(db)

    @router.get("/versions")
    def versions(event_type: str | None = None, channel: str | None = None,
                 status: str | None = None, limit: int = Query(100, le=1000)):
        """模板版本历史（draft/published/rejected）。"""
        return list_versions(db, event_type=event_type, channel=channel,
                             status=status, limit=limit)

    @router.get("/versions/{version_id}")
    def version_detail(version_id: int):
        return get_version(db, version_id)

    @router.post("/versions/{version_id}/preview")
    def version_preview(version_id: int, req: PreviewRequest):
        """对任意已存在版本（含历史 published/rejected）只读预览。"""
        return preview_version(db, version_id, req, settings)

    @router.get("/render-failures")
    def render_failures(recipient: str | None = None, event_type: str | None = None,
                        channel: str | None = None, reason_code: str | None = None,
                        resolved: bool | None = False,
                        send_task_id: int | None = None,
                        limit: int = Query(100, le=1000)):
        """渲染失败记录查询（默认只看未解除；可按原因/接收人/通道过滤）。"""
        return list_failures(db, resolved=resolved, recipient=recipient,
                             event_type=event_type, channel=channel,
                             reason_code=reason_code, send_task_id=send_task_id,
                             limit=limit)

    @router.post("/tasks/{task_id}/retry-render")
    def retry_render(task_id: int, req: RetryRenderRequest):
        """渲染失败的发送任务在修复模板/变量后重新渲染（不自动发送，由 worker 派发）。"""
        return retry_task_render(db, task_id, req, settings)

    return router
