import asyncio
import contextlib
import json
import os
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Type

from src.chat.message_receive.chat_stream import get_chat_manager
from src.common.logger import get_logger
from src.plugin_system import (
    ActionActivationType,
    BaseAction,
    BaseCommand,
    BaseEventHandler,
    BasePlugin,
    ComponentInfo,
    ConfigField,
    EventType,
    register_plugin,
)
from src.plugin_system.apis import send_api


logger = get_logger("Mai_Plan")

DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TASK_FILE_VERSION = "1.0.0"

TASK_STATUS_PENDING = "pending"
TASK_STATUS_SENT = "sent"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"

_plugin_instance: Optional["MaiPlanPlugin"] = None


class CreatePlanTaskAction(BaseAction):
    """创建计划任务动作"""

    action_name = "create_plan_task"
    action_description = "根据聊天内容创建计划任务，并在指定时间提醒用户"
    activation_type = ActionActivationType.ALWAYS
    parallel_action = True
    action_parameters = {
        "task_content": "任务内容（简洁明确）",
        "remind_time": f"提醒时间，格式 {DEFAULT_TIME_FORMAT}",
    }
    action_require = [
        "当用户表达未来提醒诉求时使用",
        "remind_time 必须是绝对时间字符串",
        "task_content 不能为空",
    ]
    associated_types = ["text"]

    async def execute(self) -> Tuple[bool, str]:
        global _plugin_instance

        if _plugin_instance is None:
            logger.warning("[Mai_Plan] 插件实例未初始化，无法创建计划任务")
            return False, "插件未初始化"

        if not _plugin_instance.is_scope_enabled(self.is_group):
            return False, "当前会话类型未启用计划任务"

        task_content = _plugin_instance.extract_task_content(self.action_data)
        remind_time = _plugin_instance.extract_remind_time(self.action_data)

        if not task_content or not remind_time:
            error_text = "创建计划任务失败：缺少 task_content 或 remind_time"
            if _plugin_instance.get_config("reminder.notify_on_create_fail", False):
                await self.send_text(error_text)
            return False, error_text

        source_message_id = ""
        if self.action_message and self.action_message.message_id:
            source_message_id = str(self.action_message.message_id)

        success, reply_text, _ = await _plugin_instance.create_task(
            chat_id=self.chat_id,
            creator_user_id=str(self.user_id or ""),
            creator_name=str(self.user_nickname or "未知用户"),
            content=task_content,
            remind_time_str=remind_time,
            source_message_id=source_message_id,
            platform=str(self.platform or ""),
            is_group=self.is_group,
        )

        if success:
            await self.send_text(reply_text)
            return True, reply_text

        if _plugin_instance.get_config("reminder.notify_on_create_fail", False):
            await self.send_text(reply_text)
        return False, reply_text


class MaiPlanCommand(BaseCommand):
    """计划任务管理命令"""

    command_name = "mai_plan_command"
    command_description = "管理计划任务：/mai_plan help|list|cancel"
    command_pattern = r"^/mai_plan(?:\s+(?P<args>.*))?$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        global _plugin_instance

        if _plugin_instance is None:
            await self.send_text("Mai_Plan 插件尚未初始化")
            return False, "插件未初始化", 2

        if not self.message.chat_stream or not self.message.chat_stream.stream_id:
            await self.send_text("无法获取当前会话信息")
            return False, "会话信息缺失", 2

        args = (self.matched_groups.get("args") or "").strip() if self.matched_groups else ""
        if not args:
            await self.send_text(self._build_help_text())
            return True, None, 2

        segments = args.split()
        subcommand = segments[0].lower()

        if subcommand in {"help", "-h", "--help"}:
            await self.send_text(self._build_help_text())
            return True, None, 2

        if subcommand == "list":
            mode = "pending"
            if len(segments) > 1:
                mode = segments[1].lower()

            if mode not in {"pending", "all"}:
                await self.send_text("参数错误：list 仅支持 pending 或 all")
                return False, "list 参数错误", 2

            tasks = await _plugin_instance.list_tasks(
                chat_id=self.message.chat_stream.stream_id,
                include_done=(mode == "all"),
            )
            await self.send_text(self._format_task_list(tasks, mode))
            return True, None, 2

        if subcommand == "cancel":
            if len(segments) < 2:
                await self.send_text("用法：/mai_plan cancel <task_id>")
                return False, "缺少 task_id", 2

            task_id = segments[1].strip()
            operator_user_id = str(self.message.message_info.user_info.user_id)
            is_admin = _plugin_instance.is_admin(operator_user_id)

            success, text = await _plugin_instance.cancel_task(
                chat_id=self.message.chat_stream.stream_id,
                task_id=task_id,
                operator_user_id=operator_user_id,
                is_admin=is_admin,
            )
            await self.send_text(text)
            return success, text, 2

        await self.send_text("未知子命令，请使用 /mai_plan help 查看帮助")
        return False, "未知子命令", 2

    def _build_help_text(self) -> str:
        return "\n".join(
            [
                "Mai_Plan 命令帮助",
                "- /mai_plan help                查看帮助",
                "- /mai_plan list [pending|all] 查看当前会话任务",
                "- /mai_plan cancel <task_id>   取消任务",
            ]
        )

    def _format_task_list(self, tasks: List[Dict[str, Any]], mode: str) -> str:
        if not tasks:
            if mode == "all":
                return "当前会话暂无任何计划任务"
            return "当前会话暂无待处理计划任务"

        lines = [f"当前会话任务列表（{len(tasks)} 条，模式：{mode}）"]
        max_show = 20

        for index, task in enumerate(tasks[:max_show], start=1):
            task_id = str(task.get("task_id", "-"))
            content = str(task.get("content", "-"))
            remind_at = str(task.get("remind_at", "-"))
            status_text = MaiPlanPlugin.status_to_text(str(task.get("status", "")))
            lines.append(f"{index}. [{task_id}] {content} | {remind_at} | {status_text}")

        if len(tasks) > max_show:
            lines.append(f"... 还有 {len(tasks) - max_show} 条任务未显示")

        return "\n".join(lines)


class MaiPlanStartupHandler(BaseEventHandler):
    """启动时启动调度器"""

    event_type = EventType.ON_START
    handler_name = "mai_plan_startup_handler"
    handler_description = "Mai_Plan 启动时初始化提醒调度器"
    weight = 0
    intercept_message = False

    async def execute(self, message: Optional[Any]) -> Tuple[bool, bool, Optional[str], None, None]:
        global _plugin_instance

        if _plugin_instance is None:
            logger.warning("[Mai_Plan] 收到 ON_START，但插件实例为空")
            return False, True, "插件实例为空", None, None

        await _plugin_instance.start_scheduler()
        return True, True, "Mai_Plan 调度器已启动", None, None


class MaiPlanStopHandler(BaseEventHandler):
    """停止时停止调度器"""

    event_type = EventType.ON_STOP
    handler_name = "mai_plan_stop_handler"
    handler_description = "Mai_Plan 停止时关闭提醒调度器"
    weight = 0
    intercept_message = False

    async def execute(self, message: Optional[Any]) -> Tuple[bool, bool, Optional[str], None, None]:
        global _plugin_instance

        if _plugin_instance is None:
            return True, True, "插件实例为空，跳过停止", None, None

        await _plugin_instance.stop_scheduler()
        return True, True, "Mai_Plan 调度器已停止", None, None


@register_plugin
class MaiPlanPlugin(BasePlugin):
    """Mai_Plan 插件：自动创建计划任务并定时提醒"""

    plugin_name = "mai_plan_plugin"
    enable_plugin = True
    dependencies: List[str] = []
    python_dependencies: List[str] = []
    config_file_name = "config.toml"

    config_section_descriptions = {
        "plugin": "插件基础配置",
        "scope": "会话范围配置",
        "time": "时间格式配置",
        "scheduler": "调度器配置",
        "reminder": "提醒文案与发送策略",
        "storage": "任务存储配置",
        "permission": "权限配置",
    }

    config_schema: Dict[str, Dict[str, ConfigField]] = {
        "plugin": {
            "enabled": ConfigField(type=bool, default=True, description="是否启用插件"),
            "config_version": ConfigField(type=str, default="1.0.0", description="配置文件版本"),
        },
        "scope": {
            "group": ConfigField(type=bool, default=True, description="是否在群聊中生效"),
            "private": ConfigField(type=bool, default=True, description="是否在私聊中生效"),
        },
        "time": {
            "format": ConfigField(type=str, default=DEFAULT_TIME_FORMAT, description="任务时间格式"),
        },
        "scheduler": {
            "tick_seconds": ConfigField(type=int, default=10, description="扫描间隔（秒）"),
            "min_future_seconds": ConfigField(type=int, default=30, description="最小提前创建秒数"),
            "max_batch_per_tick": ConfigField(type=int, default=10, description="每轮最大提醒数量"),
            "max_retry_count": ConfigField(type=int, default=3, description="提醒发送最大重试次数"),
        },
        "reminder": {
            "send_mode": ConfigField(
                type=str,
                default="origin_chat",
                description="提醒发送模式",
                choices=["origin_chat", "private_first"],
            ),
            "prefix": ConfigField(type=str, default="⏰ 日程提醒", description="提醒消息前缀"),
            "notify_on_create_fail": ConfigField(type=bool, default=False, description="创建失败是否发送提示"),
        },
        "storage": {
            "tasks_file_name": ConfigField(type=str, default="plan_tasks.json", description="任务文件名"),
        },
        "permission": {
            "admin_user_ids": ConfigField(type=list, default=[], description="管理员用户 ID 列表"),
        },
    }

    def __init__(self, *args, **kwargs):
        global _plugin_instance

        super().__init__(*args, **kwargs)
        self._tasks_lock = asyncio.Lock()
        self._scheduler_task: Optional[asyncio.Task] = None
        _plugin_instance = self

    def get_plugin_components(self) -> List[Tuple[ComponentInfo, Type]]:
        return [
            (CreatePlanTaskAction.get_action_info(), CreatePlanTaskAction),
            (MaiPlanCommand.get_command_info(), MaiPlanCommand),
            (MaiPlanStartupHandler.get_handler_info(), MaiPlanStartupHandler),
            (MaiPlanStopHandler.get_handler_info(), MaiPlanStopHandler),
        ]

    @staticmethod
    def status_to_text(status: str) -> str:
        status_map = {
            TASK_STATUS_PENDING: "待提醒",
            TASK_STATUS_SENT: "已提醒",
            TASK_STATUS_FAILED: "失败",
            TASK_STATUS_CANCELLED: "已取消",
        }
        return status_map.get(status, status or "未知")

    @staticmethod
    def _safe_int(value: Any, default: int, minimum: int = 0) -> int:
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, parsed)

    def _get_time_format(self) -> str:
        time_format = self.get_config("time.format", DEFAULT_TIME_FORMAT)
        if not isinstance(time_format, str) or not time_format.strip():
            return DEFAULT_TIME_FORMAT
        return time_format.strip()

    def _format_now(self) -> str:
        return datetime.now().strftime(DEFAULT_TIME_FORMAT)

    def _get_tasks_file_path(self) -> Path:
        file_name = self.get_config("storage.tasks_file_name", "plan_tasks.json")
        if not isinstance(file_name, str) or not file_name.strip():
            file_name = "plan_tasks.json"
        safe_name = Path(file_name.strip()).name
        return Path(self.plugin_dir) / safe_name

    def _empty_tasks_document(self) -> Dict[str, Any]:
        return {"version": TASK_FILE_VERSION, "tasks": []}

    def _normalize_tasks_document(self, data: Any) -> Dict[str, Any]:
        if not isinstance(data, dict):
            return self._empty_tasks_document()

        tasks = data.get("tasks", [])
        if not isinstance(tasks, list):
            tasks = []

        normalized_tasks: List[Dict[str, Any]] = []
        for item in tasks:
            if isinstance(item, dict) and item.get("task_id"):
                normalized_tasks.append(item)

        version = str(data.get("version", TASK_FILE_VERSION))
        return {"version": version, "tasks": normalized_tasks}

    def _read_tasks_document_unlocked(self) -> Dict[str, Any]:
        tasks_file = self._get_tasks_file_path()
        if not tasks_file.exists():
            return self._empty_tasks_document()

        try:
            with tasks_file.open("r", encoding="utf-8") as file:
                data = json.load(file)
            return self._normalize_tasks_document(data)
        except Exception as error:
            logger.error(f"[Mai_Plan] 读取任务文件失败：{error}")
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_path = tasks_file.with_name(f"{tasks_file.stem}.broken_{timestamp}{tasks_file.suffix}")
            try:
                shutil.move(str(tasks_file), str(backup_path))
                logger.warning(f"[Mai_Plan] 已将损坏任务文件备份到: {backup_path}")
            except Exception as backup_error:
                logger.error(f"[Mai_Plan] 备份损坏任务文件失败：{backup_error}")
            return self._empty_tasks_document()

    def _write_tasks_document_unlocked(self, document: Dict[str, Any]) -> None:
        tasks_file = self._get_tasks_file_path()
        tasks_file.parent.mkdir(parents=True, exist_ok=True)

        normalized = self._normalize_tasks_document(document)
        tmp_file = tasks_file.with_suffix(f"{tasks_file.suffix}.tmp")

        try:
            with tmp_file.open("w", encoding="utf-8") as file:
                json.dump(normalized, file, ensure_ascii=False, indent=2)
            os.replace(tmp_file, tasks_file)
        finally:
            if tmp_file.exists():
                with contextlib.suppress(Exception):
                    tmp_file.unlink()

    def _parse_remind_datetime(self, remind_time_str: str) -> Tuple[Optional[datetime], str]:
        value = (remind_time_str or "").strip()
        if not value:
            return None, "提醒时间不能为空"

        formats: List[str] = [self._get_time_format()]
        if DEFAULT_TIME_FORMAT not in formats:
            formats.append(DEFAULT_TIME_FORMAT)

        for time_format in formats:
            try:
                parsed = datetime.strptime(value, time_format)
                return parsed, ""
            except ValueError:
                continue

        return None, f"提醒时间格式错误，请使用 {DEFAULT_TIME_FORMAT}"

    def _generate_task_id(self, existing_ids: set[str]) -> str:
        for _ in range(10):
            task_id = f"p_{datetime.now().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex[:8]}"
            if task_id not in existing_ids:
                return task_id
        return f"p_{uuid.uuid4().hex}"

    def _resolve_target_stream_id(self, task: Dict[str, Any]) -> str:
        origin_stream_id = str(task.get("chat_id", ""))
        send_mode = str(self.get_config("reminder.send_mode", "origin_chat") or "origin_chat").strip().lower()

        if send_mode != "private_first":
            return origin_stream_id

        platform = str(task.get("platform", "")).strip()
        user_id = str(task.get("creator_user_id", "")).strip()

        if not platform or not user_id:
            return origin_stream_id

        private_stream_id = get_chat_manager().get_stream_id(platform=platform, id=user_id, is_group=False)
        if get_chat_manager().get_stream(private_stream_id):
            return private_stream_id

        return origin_stream_id

    def is_scope_enabled(self, is_group: bool) -> bool:
        if is_group:
            return bool(self.get_config("scope.group", True))
        return bool(self.get_config("scope.private", True))

    def is_admin(self, user_id: str) -> bool:
        admin_ids = self.get_config("permission.admin_user_ids", [])
        if not isinstance(admin_ids, list):
            return False
        normalized = {str(item).strip() for item in admin_ids if str(item).strip()}
        return str(user_id).strip() in normalized

    def extract_task_content(self, action_data: Dict[str, Any]) -> str:
        keys = ["task_content", "content", "task", "todo", "plan_content", "reminder_content"]
        for key in keys:
            value = action_data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def extract_remind_time(self, action_data: Dict[str, Any]) -> str:
        keys = ["remind_time", "remind_at", "time", "schedule_time", "deadline"]
        for key in keys:
            value = action_data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    async def create_task(
        self,
        chat_id: str,
        creator_user_id: str,
        creator_name: str,
        content: str,
        remind_time_str: str,
        source_message_id: str = "",
        platform: str = "",
        is_group: bool = False,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        if not chat_id:
            return False, "创建任务失败：缺少 chat_id", None

        content = content.strip()
        if not content:
            return False, "创建任务失败：任务内容不能为空", None

        remind_dt, parse_error = self._parse_remind_datetime(remind_time_str)
        if remind_dt is None:
            return False, parse_error, None

        remind_timestamp = remind_dt.timestamp()
        min_future_seconds = self._safe_int(self.get_config("scheduler.min_future_seconds", 30), 30, 0)
        if remind_timestamp < time.time() + min_future_seconds:
            return False, f"创建失败：提醒时间必须晚于当前时间至少 {min_future_seconds} 秒", None

        remind_at = remind_dt.strftime(DEFAULT_TIME_FORMAT)
        now_text = self._format_now()

        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            tasks = document["tasks"]

            for task in tasks:
                if str(task.get("chat_id", "")) != chat_id:
                    continue
                if str(task.get("content", "")).strip() != content:
                    continue
                if str(task.get("remind_at", "")) != remind_at:
                    continue
                task_status = str(task.get("status", ""))
                if task_status in {TASK_STATUS_PENDING, TASK_STATUS_SENT}:
                    task_id = str(task.get("task_id", "-"))
                    return False, f"创建失败：相同提醒已存在（任务ID：{task_id}）", task

            existing_ids = {str(task.get("task_id", "")) for task in tasks}
            task_id = self._generate_task_id(existing_ids)

            task = {
                "task_id": task_id,
                "chat_id": chat_id,
                "platform": platform,
                "is_group": is_group,
                "creator_user_id": creator_user_id,
                "creator_name": creator_name,
                "content": content,
                "remind_at": remind_at,
                "remind_ts": remind_timestamp,
                "status": TASK_STATUS_PENDING,
                "created_at": now_text,
                "triggered_at": None,
                "cancelled_at": None,
                "last_attempt_at": None,
                "retry_count": 0,
                "last_error": "",
                "source_message_id": source_message_id,
            }

            tasks.append(task)
            tasks.sort(key=lambda item: float(item.get("remind_ts", 0.0)))
            self._write_tasks_document_unlocked(document)

        reply_text = "\n".join(
            [
                f"已创建计划任务：{content}",
                f"提醒时间：{remind_at}",
                f"任务ID：{task_id}",
            ]
        )
        return True, reply_text, task

    async def list_tasks(self, chat_id: str, include_done: bool = False) -> List[Dict[str, Any]]:
        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()

        tasks = [task for task in document["tasks"] if str(task.get("chat_id", "")) == chat_id]

        if not include_done:
            visible_status = {TASK_STATUS_PENDING, TASK_STATUS_FAILED}
            tasks = [task for task in tasks if str(task.get("status", "")) in visible_status]

        tasks.sort(key=lambda item: float(item.get("remind_ts", 0.0)))
        return tasks

    async def cancel_task(
        self,
        chat_id: str,
        task_id: str,
        operator_user_id: str,
        is_admin: bool = False,
    ) -> Tuple[bool, str]:
        if not task_id:
            return False, "取消失败：task_id 不能为空"

        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            tasks = document["tasks"]

            target_task: Optional[Dict[str, Any]] = None
            for task in tasks:
                if str(task.get("chat_id", "")) == chat_id and str(task.get("task_id", "")) == task_id:
                    target_task = task
                    break

            if target_task is None:
                return False, f"取消失败：未找到任务 {task_id}"

            status = str(target_task.get("status", ""))
            if status == TASK_STATUS_SENT:
                return False, f"取消失败：任务 {task_id} 已触发提醒"
            if status == TASK_STATUS_CANCELLED:
                return False, f"任务 {task_id} 已经是取消状态"

            owner_user_id = str(target_task.get("creator_user_id", ""))
            if not is_admin and owner_user_id != str(operator_user_id):
                return False, "取消失败：仅任务创建者或管理员可取消"

            target_task["status"] = TASK_STATUS_CANCELLED
            target_task["cancelled_at"] = self._format_now()
            target_task["last_error"] = ""
            self._write_tasks_document_unlocked(document)

        return True, f"已取消任务 {task_id}"

    async def start_scheduler(self) -> None:
        if not bool(self.get_config("plugin.enabled", True)):
            logger.info("[Mai_Plan] 插件配置为禁用，跳过调度器启动")
            return

        if self._scheduler_task and not self._scheduler_task.done():
            return

        self._scheduler_task = asyncio.create_task(self._scheduler_loop())
        with contextlib.suppress(Exception):
            self._scheduler_task.set_name("Mai_Plan_scheduler")
        logger.info("[Mai_Plan] 提醒调度器已启动")

    async def stop_scheduler(self) -> None:
        task = self._scheduler_task
        self._scheduler_task = None

        if task and not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        logger.info("[Mai_Plan] 提醒调度器已停止")

    async def _scheduler_loop(self) -> None:
        try:
            while True:
                try:
                    await self._scan_due_tasks()
                except Exception as error:
                    logger.error(f"[Mai_Plan] 扫描到期任务失败：{error}", exc_info=True)

                tick_seconds = self._safe_int(self.get_config("scheduler.tick_seconds", 10), 10, 1)
                await asyncio.sleep(tick_seconds)
        except asyncio.CancelledError:
            logger.info("[Mai_Plan] 调度器任务收到取消信号")
            raise

    async def _scan_due_tasks(self) -> None:
        now_timestamp = time.time()
        batch_limit = self._safe_int(self.get_config("scheduler.max_batch_per_tick", 10), 10, 1)

        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            due_tasks = [
                task.copy()
                for task in document["tasks"]
                if str(task.get("status", "")) == TASK_STATUS_PENDING
                and float(task.get("remind_ts", 0.0)) <= now_timestamp
            ]

        if not due_tasks:
            return

        due_tasks.sort(key=lambda item: float(item.get("remind_ts", 0.0)))
        due_tasks = due_tasks[:batch_limit]

        for due_task in due_tasks:
            task_id = str(due_task.get("task_id", ""))
            if not task_id:
                continue

            success, error_text = await self._send_due_task(due_task)
            update_time = self._format_now()

            async with self._tasks_lock:
                latest_document = self._read_tasks_document_unlocked()
                latest_tasks = latest_document["tasks"]

                target_task = None
                for task in latest_tasks:
                    if str(task.get("task_id", "")) == task_id:
                        target_task = task
                        break

                if not target_task:
                    continue

                if str(target_task.get("status", "")) != TASK_STATUS_PENDING:
                    continue

                target_task["last_attempt_at"] = update_time
                if success:
                    target_task["status"] = TASK_STATUS_SENT
                    target_task["triggered_at"] = update_time
                    target_task["last_error"] = ""
                else:
                    current_retry = self._safe_int(target_task.get("retry_count", 0), 0, 0)
                    current_retry += 1
                    target_task["retry_count"] = current_retry
                    target_task["last_error"] = error_text

                    max_retry = self._safe_int(self.get_config("scheduler.max_retry_count", 3), 3, 0)
                    if current_retry > max_retry:
                        target_task["status"] = TASK_STATUS_FAILED

                self._write_tasks_document_unlocked(latest_document)

    async def _send_due_task(self, task: Dict[str, Any]) -> Tuple[bool, str]:
        task_id = str(task.get("task_id", ""))
        chat_id = str(task.get("chat_id", ""))
        content = str(task.get("content", "")).strip()
        remind_at = str(task.get("remind_at", "")).strip()

        if not chat_id:
            return False, f"任务 {task_id} 缺少 chat_id"
        if not content:
            return False, f"任务 {task_id} 缺少 content"

        prefix = self.get_config("reminder.prefix", "⏰ 日程提醒")
        if not isinstance(prefix, str) or not prefix.strip():
            prefix = "⏰ 日程提醒"

        target_stream_id = self._resolve_target_stream_id(task)
        if not target_stream_id:
            return False, f"任务 {task_id} 缺少可用 stream_id"

        message_text = "\n".join(
            [
                str(prefix).strip(),
                f"任务：{content}",
                f"计划时间：{remind_at or '-'}",
                f"任务ID：{task_id}",
            ]
        )

        success = await send_api.text_to_stream(
            text=message_text,
            stream_id=target_stream_id,
            storage_message=True,
        )

        if success:
            return True, ""

        return False, f"任务 {task_id} 发送提醒失败（stream_id={target_stream_id}）"
