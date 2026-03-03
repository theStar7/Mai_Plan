import asyncio
import contextlib
import json
import os
import re
import shutil
import time
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Type

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger
from apscheduler.triggers.interval import IntervalTrigger

from src.chat.message_receive.chat_stream import get_chat_manager
from src.plugin_system.apis import llm_api
from src.config.config import global_config, model_config



from src.common.logger import get_logger
from src.plugin_system import (
    BaseCommand,
    BaseEventHandler,
    BasePlugin,
    BaseTool,
    ComponentInfo,
    ConfigField,
    EventType,
    ToolParamType,
    register_plugin,
)
from src.plugin_system.apis import send_api
from src.plugin_system.apis import generator_api



logger = get_logger("Mai_Plan")

DEFAULT_TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
TASK_FILE_VERSION = "1.0.0"

TASK_STATUS_PENDING = "pending"
TASK_STATUS_SENT = "sent"
TASK_STATUS_FAILED = "failed"
TASK_STATUS_CANCELLED = "cancelled"

SCHEDULE_TYPE_ONCE = "once"
SCHEDULE_TYPE_CRON = "cron"
SCHEDULE_TYPE_INTERVAL = "interval"

# CronTrigger 关键字参数白名单
CRON_KWARGS_ALLOWED_KEYS = {"year", "month", "day", "week", "day_of_week", "hour", "minute", "second"}

_plugin_instance: Optional["MaiPlanPlugin"] = None


ToolResultStatus = Literal["成功", "失败", "部分成功"]


def _split_nonempty_lines(text: str) -> List[str]:
    """将文本按行拆分，返回去空白且非空的行。"""
    if not text:
        return []
    return [line.strip() for line in str(text).splitlines() if line.strip()]


def _format_tool_result(
    tool_name: str,
    action_name: str,
    status: ToolResultStatus,
    summary: str,
    details: Optional[List[str]] = None,
    other_info: str = "",
) -> str:
    """统一格式化 Tool 的结构化执行结果文本。"""
    lines = [
        f"执行结果：{status}",
        f"工具：{tool_name}",
        f"动作：{action_name}",
        f"结果摘要：{summary}",
    ]

    normalized_details = [str(item).strip() for item in (details or []) if str(item).strip()]
    if normalized_details:
        lines.append("结果详情：")
        lines.extend(f"- {item}" for item in normalized_details)

    if other_info.strip():
        lines.append(f"其他信息：{other_info.strip()}")

    return "\n".join(lines)


class MaiPlanCommand(BaseCommand):
    """计划任务管理命令"""

    command_name = "mai_plan_command"
    command_description = "管理计划任务：/mai_plan help|list|cancel"
    command_pattern = r"^/mai_plan(?:\s+(?P<args>.*))?$"

    async def execute(self) -> Tuple[bool, Optional[str], int]:
        """
        执行计划任务管理命令。
        支持 help、list、cancel 三个子命令。
        
        Returns:
            Tuple[bool, Optional[str], int]: (成功标志, 日志消息, 优先级)
        """
        global _plugin_instance

        if _plugin_instance is None:
            await self.send_text("Mai_Plan 插件尚未初始化", storage_message = False)
            return False, "插件未初始化", 2

        if not self.message.chat_stream or not self.message.chat_stream.stream_id:
            await self.send_text("无法获取当前会话信息", storage_message = False)
            return False, "会话信息缺失", 2

        args = (self.matched_groups.get("args") or "").strip() if self.matched_groups else ""
        if not args:
            await self.send_text(self._build_help_text(), storage_message = False)
            return True, None, 2

        segments = args.split()
        subcommand = segments[0].lower()

        operator_user_id = str(self.message.message_info.user_info.user_id)
        is_admin = _plugin_instance.is_admin(operator_user_id)
        
        if subcommand in {"help", "-h", "--help"}:
            await self.send_text(self._build_help_text(), storage_message = False)
            return True, None, 2

        if subcommand == "list":
            mode = "pending"
            if len(segments) > 1:
                mode = segments[1].lower()

            if mode not in {"pending", "all"}:
                await self.send_text("参数错误", storage_message = False)
                return False, "list 参数错误", 2
            
            if mode == "all" and not is_admin:
                await self.send_text("权限不足，无法查看全部任务", storage_message = False)
                return False, "权限不足查看全部任务", 2

            tasks = await _plugin_instance.list_tasks(
                chat_id=self.message.chat_stream.stream_id,
                include_all_tasks=(mode == "all"),
            )

            await self.send_text(self._format_task_list(tasks, mode), storage_message = False)
            return True, None, 2

        if subcommand == "cancel":
            if len(segments) < 2:
                await self.send_text("用法：/mai_plan cancel <task_id>", storage_message = False)
                return False, "缺少 task_id", 2

            task_id = segments[1].strip()

            success, text = await _plugin_instance.cancel_task(
                chat_id=self.message.chat_stream.stream_id,
                task_id=task_id,
                operator_user_id=operator_user_id,
                is_admin=is_admin,
            )
            await self.send_text(text, storage_message = False  )
            
            return success, text, 2

        await self.send_text("未知子命令，请使用 /mai_plan help 查看帮助", storage_message = False)
        return False, "未知子命令", 2

    def _build_help_text(self) -> str:
        """
        生成帮助文本。
        
        Returns:
            str: 格式化的帮助信息
        """
        return "\n".join(
            [
                "Mai_Plan 命令帮助",
                "- /mai_plan help                查看帮助",
                "- /mai_plan list [pending|all] 查看当前会话任务",
                "- /mai_plan cancel <task_id>   取消任务",
            ]
        )

    def _format_task_list(self, tasks: List[Dict[str, Any]], mode: str) -> str:
        """
        格式化任务列表为可读的文字。
        最多显示 20 条任务。
        
        Args:
            tasks: 任务列表
            mode: 显示模式（pending 或 all）
            
        Returns:
            str: 格式化后的任务列表文本
        """
        if not tasks:
            if mode == "all":
                return "暂无任何计划任务"
            return "暂无待处理计划任务"

        lines = [f"当前任务列表（{len(tasks)} 条，模式：{mode}）"]
        max_show = 20

        for index, task in enumerate(tasks[:max_show], start=1):
            task_id = str(task.get("task_id", "-"))
            content = str(task.get("content", "-"))
            remind_at = str(task.get("remind_at", "-"))
            creator_name = str(task.get("creator_name", "-"))
            status_text = MaiPlanPlugin.status_to_text(str(task.get("status", "")))
            stype = task.get("schedule_type", "")
            if stype == SCHEDULE_TYPE_CRON:
                cron_desc = MaiPlanPlugin._describe_cron_kwargs(task.get("cron_kwargs", {}))
                occ = task.get("occurrence_count", 0)
                lines.append(
                    f"{index}. \U0001f501 [{task_id}] {content} | {cron_desc} | 创建者: {creator_name} | 下次: {remind_at} | 已触发{occ}次"
                )
            elif stype == SCHEDULE_TYPE_INTERVAL:
                interval_desc = MaiPlanPlugin._describe_interval(task.get("interval_seconds", 0))
                occ = task.get("occurrence_count", 0)
                lines.append(
                    f"{index}. \U0001f501 [{task_id}] {content} | {interval_desc} | 创建者: {creator_name} | 下次: {remind_at} | 已触发{occ}次"
                )
            else:
                lines.append(f"{index}. [{task_id}] {content} | 创建者: {creator_name} | {remind_at} | {status_text}")

        if len(tasks) > max_show:
            lines.append(f"... 还有 {len(tasks) - max_show} 条任务未显示")

        return "\n".join(lines)


class CreatePlanTaskTool(BaseTool):
    """创建计划任务工具（供 LLM 调用），支持一次性、cron 循环和 interval 固定间隔三种模式"""
    name = "create_plan_task_tool"
    description = (
        "核心功能：创建未来的计划提醒或主动关怀任务，支持一次性（once）、cron 循环（cron）和固定间隔（interval）三种模式。触发场景：\n"
        "1. 【响应指令】用户明确要求提醒、安排日程或待办事项时。包括：\n"
        "   - 绝对时间：'明天早上8点提醒我'、'3月5号下午3点叫我'\n"
        "   - 相对时间间隔：'2小时后提醒我'、'半小时后叫我'、'10分钟后提醒'——须根据当前时间自行换算为绝对时间填入 remind_time。\n"
        "2. 【智能关怀】从对话中感知到用户未来有重要事件（生日、考试、出行等），主动推算时间创建任务。\n"
        "3. 【话题跟进】用户提及未来公共事件（电影上映、游戏发售等），主动创建提醒。\n"
        "4. 【话题延续】话题未尽或约定改天再聊时，创建续聊任务。\n"
        "5. 【cron 循环日程】用户表达周期性的循环提醒需求时（如每天、每周几、每月几号等），设置 schedule_type='cron' 并提供 cron_kwargs。\n"
        "6. 【固定间隔】用户表达固定间隔重复提醒需求（如每隔30分钟、每2小时提醒一次），设置 schedule_type='interval' 并提供 interval_expr。\n"
    )
    parameters = [
        (
            "task_content",
            ToolParamType.STRING,
            "Bot在提醒时间点需要提醒的内容, 或者填入bot在未来需要执行的具体行为。请保证内容简洁明了",
            True,
            None,
        ),
        (
            "remind_time",
            ToolParamType.STRING,
            f"触发任务的准确时间点，格式为 {DEFAULT_TIME_FORMAT}。一次性任务(schedule_type='once')必填；"
            "cron/interval 循环任务可留空（首次触发自动计算）。"
            "当用户使用相对时间（如'2小时后''30分钟后'）时，须自行将当前时间加上间隔后填入此字段。",
            False,
            None,
        ),
        (
            "creator_name", 
            ToolParamType.STRING, 
            "创建计划用户的名称（可从上下文获取）",
            False, None
        ),
        #目前Tool不支持获取ID号，留着战未来
        # (
        #     "operator_id",
        #     ToolParamType.STRING,
        #     "创建者的数字ID号码（可从上下文获取）",
        #     False,
        #     None
        # ),
        (
            "schedule_type",
            ToolParamType.STRING,
            "调度类型，三选一：'once'（一次性，默认）、'cron'（cron 循环）、'interval'（固定间隔循环）。",
            False,
            None,
        ),
        (
            "cron_kwargs",
            ToolParamType.STRING,
            "CronTrigger 关键字参数的 JSON 字符串。仅 schedule_type='cron' 时必填。"
            "允许的键：year, month, day, week, day_of_week, hour, minute, second。"
            "值可以是整数或 cron 风格字符串（如 '*/5', '1-5', '1,15'）。\n"
            "常见示例：\n"
            '  每天08:00 → {"hour": 8, "minute": 0}\n'
            '  工作日09:30 → {"hour": 9, "minute": 30, "day_of_week": "mon-fri"}\n'
            '  每周一14:00 → {"hour": 14, "minute": 0, "day_of_week": "mon"}\n'
            '  每月1日10:00 → {"day": 1, "hour": 10, "minute": 0}\n'
            '  每30分钟 → {"minute": "*/30"}\n'
            '  每2小时整点 → {"hour": "*/2", "minute": 0}\n'
            "注意：返回合法 JSON 字符串，不要包含额外解释文字或 Markdown 代码块。",
            False,
            None,
        ),
        (
            "interval_expr",
            ToolParamType.STRING,
            "固定间隔表达式，仅 schedule_type='interval' 时必填。"
            "支持的单位：d（天）、h（小时）、m（分钟）、s（秒），可组合使用。\n"
            "示例：'30m'（每30分钟）、'2h'（每2小时）、'1h30m'（每1.5小时）、'1d'（每天）、'90s'（每90秒）。",
            False,
            None,
        )

    ]
    available_for_llm = True

    async def execute(self, function_args: Dict[str, Any]) -> Dict[str, Any]:
        """执行创建计划任务。"""
        global _plugin_instance
        action_name = "创建计划任务"

        if _plugin_instance is None:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "Mai_Plan 插件未初始化",
                ),
            }

        if not self.chat_stream or not self.chat_id:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "无法获取当前会话信息",
                ),
            }

        task_content = str(function_args.get("task_content", "")).strip()
        remind_time = str(function_args.get("remind_time", "")).strip()
        creator_name = str(function_args.get("creator_name", "")).strip() or "未知用户"
        schedule_type = str(function_args.get("schedule_type", "") or "").strip().lower() or SCHEDULE_TYPE_ONCE
        cron_kwargs_raw = function_args.get("cron_kwargs")
        interval_expr = str(function_args.get("interval_expr", "") or "").strip() or None
        operator_id = str(function_args.get("operator_id", "") or "").strip() or None
        if not task_content:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "task_content 不能为空",
                    ["请提供具体的提醒内容"],
                ),
            }

        # 校验 schedule_type
        if schedule_type not in (SCHEDULE_TYPE_ONCE, SCHEDULE_TYPE_CRON, SCHEDULE_TYPE_INTERVAL):
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "schedule_type 参数无效",
                    [f"收到: {schedule_type}", "允许值: once / cron / interval"],
                ),
            }

        if schedule_type == SCHEDULE_TYPE_ONCE and not remind_time:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "一次性任务必须提供 remind_time",
                    [f"时间格式: {DEFAULT_TIME_FORMAT}"],
                ),
            }

        # 解析 cron_kwargs
        cron_kwargs: Optional[Dict[str, Any]] = None
        if schedule_type == SCHEDULE_TYPE_CRON:
            if not cron_kwargs_raw:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "cron 循环任务必须提供 cron_kwargs",
                    ),
                }
            if isinstance(cron_kwargs_raw, str):
                try:
                    cron_kwargs = json.loads(cron_kwargs_raw)
                except json.JSONDecodeError:
                    return {
                        "name": self.name,
                        "content": _format_tool_result(
                            self.name,
                            action_name,
                            "失败",
                            "cron_kwargs 必须是合法的 JSON 字符串",
                        ),
                    }
            elif isinstance(cron_kwargs_raw, dict):
                cron_kwargs = cron_kwargs_raw
            else:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "cron_kwargs 格式无效",
                    ),
                }

        if schedule_type == SCHEDULE_TYPE_INTERVAL and not interval_expr:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "interval 循环任务必须提供 interval_expr",
                    ["示例: 30m / 2h / 1h30m"],
                ),
            }

        platform = str(getattr(self.chat_stream, "platform", "") or "")
        is_group = bool(getattr(self.chat_stream, "is_group", False))

        try:
            success, reply_text, task = await _plugin_instance.create_task(
                chat_id=self.chat_id,
                creator_user_id=operator_id or "",
                creator_name=creator_name,
                content=task_content,
                remind_time_str=remind_time,
                source_message_id="",
                platform=platform,
                is_group=is_group,
                schedule_type=schedule_type,
                cron_kwargs=cron_kwargs,
                interval_expr=interval_expr,
            )
            details = [f"schedule_type: {schedule_type}"]
            if isinstance(task, dict):
                task_id = str(task.get("task_id", "")).strip()
                if task_id:
                    details.insert(0, f"task_id: {task_id}")
            details.extend(_split_nonempty_lines(reply_text))
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "成功" if success else "失败",
                    "创建成功" if success else "创建计划任务失败",
                    details,
                ),
            }
        except Exception as exc:
            logger.error(f"[Mai_Plan] CreatePlanTaskTool 执行异常：{exc}")
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "创建计划任务时发生异常",
                    [f"异常信息: {exc}"],
                ),
            }


class DeletePlanTaskTool(BaseTool):
    """删除（取消）计划任务工具（供 LLM 调用）"""

    name = "delete_plan_task_tool"
    description = (
        "用于取消用户指定的待处理计划任务和提醒任务。"
        "高优先用于处理“取消/删除提醒”的明确意图，取消当前会话中的计划任务。"
        "当用户出现以下表达应优先触发：取消提醒、删除计划、不用再提醒、这个提醒作废、先关掉这个任务、这件事已经做完不用提醒。"
    )
    parameters = [
        (
            "task_content",
            ToolParamType.STRING,
            "要取消的任务线索，建议包含 task_id 或完整内容+提醒时间。工具将高精度匹配，线索不充分时会拒绝执行以避免误删。",
            True,
            None,
        ),
        (
            "operator_name",
            ToolParamType.STRING,
            "执行取消操作的用户的昵称（可从上下文获取）",
            False,
            None,
        )
    ]
    available_for_llm = True

    async def execute(self, function_args: Dict[str, Any]) -> Dict[str, Any]:
        """根据任务内容匹配并删除计划任务。"""
        global _plugin_instance
        action_name = "取消计划任务"
        detail_request_more = [
            "请提供更详细信息以便定位任务",
            "优先提供 task_id",
            "或提供完整任务内容（动作+对象）",
            "并提供提醒时间（建议精确到分钟）",
        ]

        if _plugin_instance is None:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "Mai_Plan 插件未初始化",
                ),
            }

        if not self.chat_stream or not self.chat_id:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "无法获取当前会话信息",
                ),
            }

        task_content = str(function_args.get("task_content", "")).strip()
        operator_name = str(function_args.get("operator_name", "")).strip()
        #is_admin = _plugin_instance.is_admin(operator_id) 暂不支持获取ID，先保留等待未来更新

        if not task_content:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "task_content 不能为空",
                ),
            }
        
        is_group = bool(getattr(self.chat_stream, "is_group", False))
        try:
            # 先查询当前会话的待处理任务
            # if is_group:
            #     tasks = await _plugin_instance.list_tasks(
            #         chat_id=self.chat_id,
            #         include_all_tasks = False,
            #         creator_name = operator_user_name if operator_user_name else None
            #     )
            # else :
            tasks = await _plugin_instance.list_tasks(
                    chat_id=self.chat_id,
                    include_all_tasks=False,
                )

            if not tasks:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "暂无待处理任务，无法取消",
                    ),
                }

            explicit_ids = set(re.findall(r"\bp_[0-9a-fA-F]{6,12}\b", task_content))
            
            
            matched: List[Dict[str, Any]] = []

            if explicit_ids:
                matched = [
                    task
                    for task in tasks
                    if str(task.get("task_id", "")) in explicit_ids
                ]
            else:
                llm_success, llm_content, reasoning, model_name = await llm_api.generate_with_model(
                    "你是任务删除匹配判定器，目标是避免误删（宁可漏匹配，不可错匹配）。\n"
                    f"用户删除线索：{task_content}\n"
                    "请在“待处理任务列表”中找出与该线索“确定对应”的任务 task_id。\n"
                    "判定要求：\n"
                    "1) 仅在证据充分时返回：task_id精确命中，或任务内容的动作+对象明确一致，或提醒时间明确一致；\n"
                    "2) 若线索过于模糊/存在多种解释，返回None；\n"
                    "3) 不允许基于主题相近或联想猜测匹配。\n"
                    "输出规则：\n"
                    "1) 只输出 task_id，多个用单个空格分隔；\n"
                    "2) 不输出解释、前后缀、标点、换行或代码块；\n"
                    "3) 无法确定时输出None。\n"
                    f"待处理任务列表：{tasks}",
                    model_config.model_task_config.tool_use,
                    request_type="mai_only_you",
                )
                if llm_success and llm_content:
                    returned_ids = set(re.findall(r"\bp_[0-9a-fA-F]{6,12}\b", llm_content.strip()))
                    if not returned_ids:
                        returned_ids = {llm_content.strip()}
                    matched = [
                        task
                        for task in tasks
                        if str(task.get("task_id", "")) in returned_ids
                    ]

            if not matched:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "未找到可取消的目标任务",
                        [f"查询关键词: {task_content}", *detail_request_more],
                    ),
                }

            total = len(matched)
            success_count = 0
            fail_count = 0
            details: List[str] = []

            for task in matched:
                task_id = str(task.get("task_id", ""))
                task_text = str(task.get("content", ""))
                remind_at = str(task.get("remind_at", ""))
                task_creator = str(task.get("creator_name", ""))
                if operator_name and task_creator and operator_name != task_creator:
                    fail_count += 1
                    details.append(
                        f"[{task_id}] {task_text} | 提醒时间: {remind_at} | 结果: 失败（无权限：操作者「{operator_name}」非任务创建者「{task_creator}」）"
                    )
                    continue
                cancel_success, text = await _plugin_instance.cancel_task(
                    chat_id=self.chat_id,
                    task_id=task_id,
                    operator_user_id="",
                    is_admin=True,
                )
                if cancel_success:
                    success_count += 1
                    details.append(
                        f"[{task_id}] {task_text} | 提醒时间: {remind_at} | 结果: 成功"
                    )
                else:
                    fail_count += 1
                    details.append(
                        f"[{task_id}] {task_text} | 提醒时间: {remind_at} | 结果: 失败（{text}）"
                    )

            if success_count == total:
                status: ToolResultStatus = "成功"
            elif fail_count == total:
                status = "失败"
            else:
                status = "部分成功"

            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    status,
                    f"匹配{total}条，成功{success_count}条，失败{fail_count}条",
                    details,
                ),
            }
        except Exception as exc:
            logger.error(f"[Mai_Plan] DeletePlanTaskTool 执行异常：{exc}")
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "取消计划任务时发生异常",
                    [f"异常信息: {exc}"],
                ),
            }


class ModifyPlanTaskTool(BaseTool):
    """修改计划任务工具（供 LLM 调用）"""

    name = "modify_plan_task_tool"
    description = (
        "根据用户提供的线索，在当前会话中修改一条待处理计划任务的内容和/或提醒时间。"
        "线索可包含 task_id、任务内容关键词、提醒时间或相关事件信息。"
        "适用于用户表达修改计划的意图，如更改提醒时间、更新任务描述等"
    )
    parameters = [
        (
            "task_content",
            ToolParamType.STRING,
            "要修改的任务关键词（task_id、内容、时间等），用于模糊匹配待处理任务列表中的目标任务",
            True,
            None,
        ),
        (
            "new_content",
            ToolParamType.STRING,
            "新的任务内容描述",
            False,
            None,
        ),
        (
            "new_remind_time",
            ToolParamType.STRING,
            f"新的提醒时间，格式为 {DEFAULT_TIME_FORMAT}（不修改时间则留空或不传）",
            False,
            None,
        ),
        (
            "new_cron_kwargs",
            ToolParamType.STRING,
            "新的 CronTrigger 关键字参数 JSON 字符串（仅 cron 循环任务可用，不修改则留空或不传）",
            False,
            None,
        ),
        (
            "new_interval_expr",
            ToolParamType.STRING,
            "新的间隔时间表达式（仅 interval 任务可用，如 '30m'、'2h'，不修改则留空或不传）",
            False,
            None,
        ),
        (
            "operator_name",
            ToolParamType.STRING,
            "执行修改操作的用户的昵称（可从上下文获取）",
            False,
            None,
        )
    ]
    available_for_llm = True  # 启用修改工具

    async def execute(self, function_args: Dict[str, Any]) -> Dict[str, Any]:
        """根据任务关键词匹配并修改计划任务。"""
        global _plugin_instance
        action_name = "修改计划任务"
        detail_request_more = [
            "请提供更详细信息以便定位任务",
            "优先提供 task_id",
            "或提供完整任务内容（动作+对象）",
            "并提供提醒时间（建议精确到分钟）",
            "如为循环任务，可补充周期规则（cron/interval）",
        ]

        if _plugin_instance is None:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "Mai_Plan 插件未初始化",
                ),
            }

        if not self.chat_stream or not self.chat_id:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "无法获取当前会话信息",
                ),
            }

        task_content = str(function_args.get("task_content", "")).strip()
        if not task_content:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "task_content 不能为空",
                ),
            }

        new_content = str(function_args.get("new_content", "") or "").strip() or None
        new_remind_time = str(function_args.get("new_remind_time", "") or "").strip() or None
        new_cron_kwargs_raw = function_args.get("new_cron_kwargs")
        new_interval_expr = str(function_args.get("new_interval_expr", "") or "").strip() or None
        operator_name = str(function_args.get("operator_name", "") or "").strip() or None

        # 解析 new_cron_kwargs
        new_cron_kwargs: Optional[Dict[str, Any]] = None
        if new_cron_kwargs_raw:
            if isinstance(new_cron_kwargs_raw, str):
                try:
                    new_cron_kwargs = json.loads(new_cron_kwargs_raw)
                except json.JSONDecodeError:
                    return {
                        "name": self.name,
                        "content": _format_tool_result(
                            self.name,
                            action_name,
                            "失败",
                            "new_cron_kwargs 必须是合法的 JSON 字符串",
                        ),
                    }
            elif isinstance(new_cron_kwargs_raw, dict):
                new_cron_kwargs = new_cron_kwargs_raw

        if not new_content and not new_remind_time and not new_cron_kwargs and not new_interval_expr:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "至少需要提供 new_content、new_remind_time、new_cron_kwargs 或 new_interval_expr 中的一项",
                ),
            }

        try:
            # 在锁外完成 list_tasks 和 LLM 匹配，避免长时间持锁
            tasks = await _plugin_instance.list_tasks(
                chat_id=self.chat_id,
                include_all_tasks=False,
                creator_name=operator_name if operator_name else None
            )

            if not tasks:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "暂无待处理任务，无法修改",
                    ),
                }

            matched: List[Dict[str, Any]] = []
            llm_success, llm_content, reasoning, model_name = await llm_api.generate_with_model(
                "你是任务修改匹配判定器，目标是避免错判（宁可漏判，不可误判）。\n"
                f"用户提供的修改线索：{task_content}\n"
                "请在“待处理任务列表”中找出与线索“确定对应”的任务，并返回 task_id。\n"
                "判定规则：\n"
                "1) 若线索中包含 task_id，优先按 task_id 精确匹配；\n"
                "2) 内容匹配必须满足动作+对象一致，主题相近不算同一任务；\n"
                "3) 若线索包含时间（日期/时刻），需与 remind_at 一致或高度一致；\n"
                "4) 若仅能猜测或证据不足，返回None；\n"
                "5) 若存在多个同等明确候选，返回所有 task_id（空格分隔）。\n"
                "输出规则：\n"
                "1) 只输出 task_id 本身；\n"
                "2) 不要输出解释、前后缀、标点、换行或代码块；\n"
                "3) 若没有明确匹配项，返回None；\n"
                "4) 若有多条匹配，用空格分隔所有 task_id；\n"
                f"待处理任务列表：{tasks}",
                model_config.model_task_config.tool_use,
                request_type="mai_only_you",
            )
            if llm_success and llm_content:
                returned_ids = set(re.findall(r"\bp_[0-9a-fA-F]{6,12}\b", llm_content.strip()))
                if not returned_ids:
                    returned_ids = {llm_content.strip()}
                matched = [
                    task
                    for task in tasks
                    if str(task.get("task_id", "")) in returned_ids
                ]

            if not matched:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "未找到匹配的待处理任务，无法修改",
                        [f"查询关键词: {task_content}", *detail_request_more],
                    ),
                }

            if len(matched) > 1:
                details = []
                for idx, task in enumerate(matched, start=1):
                    tid = str(task.get("task_id", "-"))
                    tcontent = str(task.get("content", "-"))
                    tremind = str(task.get("remind_at", "-"))
                    details.append(f"{idx}. [{tid}] {tcontent} | {tremind}")
                details.extend(detail_request_more)
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "失败",
                        "匹配到多个候选，无法确定修改目标",
                        details,
                    ),
                }

            target = matched[0]
            target_task_id = str(target.get("task_id", ""))

            success, text = await _plugin_instance.update_task(
                chat_id=self.chat_id,
                task_id=target_task_id,
                new_content=new_content,
                new_remind_time_str=new_remind_time,
                new_cron_kwargs=new_cron_kwargs,
                new_interval_expr=new_interval_expr,
            )
            details = [f"target_task_id: {target_task_id}"]
            details.extend(_split_nonempty_lines(text))
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "成功" if success else "失败",
                    "任务修改成功" if success else "任务修改失败",
                    details,
                ),
            }
        except Exception as exc:
            logger.error(f"[Mai_Plan] ModifyPlanTaskTool 执行异常：{exc}")
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "修改计划任务时发生异常",
                    [f"异常信息: {exc}"],
                ),
            }


class ListPlanTasksTool(BaseTool):
    """查询计划任务列表工具（供 LLM 调用）"""

    name = "list_plan_tasks_tool"
    description = (
        "查询当前会话中的计划,任务,提醒列表。"
        "当用户只想查看任务时调用，例如“我有哪些提醒”“列出我的计划”“还有什么待办”。"
        "出现“取消/删除/作废/不用提醒”意图时，必须调用 delete_plan_task_tool；"
        "出现“修改/改时间/改内容/延期/提前”意图时，必须调用 modify_plan_task_tool；"
        "出现“创建/新增/设一个提醒”意图时，必须调用 create_plan_task_tool。"
        "若用户表达的是操作意图（增删改）而非查询意图，严禁先调用本工具。"
    )
    parameters = [
        (
            "mode",
            ToolParamType.STRING,
            "查询模式：pending（仅待处理，默认）或 all（包含已完成和已取消的全部任务）",
            False,
            None,
        ),
        (
            "inquirer_name", 
            ToolParamType.STRING, 
            "查询者的昵称（可从上下文获取）",
            False,
            None)
    ]
    available_for_llm = True  

    async def execute(self, function_args: Dict[str, Any]) -> Dict[str, Any]:
        """执行查询计划任务列表。"""
        global _plugin_instance
        action_name = "查询计划任务列表"

        if _plugin_instance is None:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "Mai_Plan 插件未初始化",
                ),
            }

        if not self.chat_stream or not self.chat_id:
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "无法获取当前会话信息",
                ),
            }
        
        inquirer_name = str(function_args.get("inquirer_name", "")).strip() or "未知用户"

        mode = "pending"
        try:
            tasks = await _plugin_instance.list_tasks(
                chat_id=self.chat_id,
                include_all_tasks=False,
            )
            if not tasks:
                return {
                    "name": self.name,
                    "content": _format_tool_result(
                        self.name,
                        action_name,
                        "成功",
                        "查询完成，当前会话暂无任务",
                        "注意: 当前工具仅做查询展示，不包含任何删除或修改功能。你并未成功执行任何删除或修改任务"

                    ),
                }

            task_list_text = self._format_task_list(tasks, mode, inquirer_name, self.chat_stream)
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "成功",
                    f"查询完成，共{len(tasks)}条任务（mode={mode}）",
                    _split_nonempty_lines(task_list_text),
                    "注意: 当前工具仅做查询展示，不包含任何删除或修改功能。你并未成功执行任何删除或修改任务"
                ),
            }
        except Exception as exc:
            logger.error(f"[Mai_Plan] ListPlanTasksTool 执行异常：{exc}")
            return {
                "name": self.name,
                "content": _format_tool_result(
                    self.name,
                    action_name,
                    "失败",
                    "查询计划任务时发生异常",
                    [f"异常信息: {exc}"],
                ),
            }

    @staticmethod
    def _format_task_list(tasks: List[Dict[str, Any]], mode: str, inquirer_name, chat_stream) -> str:
        """将任务列表格式化为可读文本。"""
        if not tasks:
            return ""

        lines: List[str] = []
        max_show = 20

        is_group = bool(getattr(chat_stream, "is_group", False)) # 群聊中仅显示自己创建的任务，私聊中显示全部任务

        for index, task in enumerate(tasks[:max_show], start=1):
            task_id = str(task.get("task_id", "-"))
            content = str(task.get("content", "-"))
            remind_at = str(task.get("remind_at", "-"))
            creator_name = str(task.get("creator_name", "-"))
            status = str(task.get("status", ""))
            status_map = {
                TASK_STATUS_PENDING: "待提醒",
                TASK_STATUS_SENT: "已提醒",
                TASK_STATUS_FAILED: "失败",
                TASK_STATUS_CANCELLED: "已取消",
            }
            status_text = status_map.get(status, status or "未知")

            if not is_group or (creator_name == inquirer_name and inquirer_name != "未知用户"):
                stype = task.get("schedule_type", "")
                if stype == SCHEDULE_TYPE_CRON:
                    cron_desc = MaiPlanPlugin._describe_cron_kwargs(task.get("cron_kwargs", {}))
                    occ = task.get("occurrence_count", 0)
                    lines.append(
                        f"{index}. \U0001f501 [{task_id}] {content} | {cron_desc} | 创建者: {creator_name} | 下次: {remind_at} | 已触发{occ}次"
                    )
                elif stype == SCHEDULE_TYPE_INTERVAL:
                    interval_desc = MaiPlanPlugin._describe_interval(task.get("interval_seconds", 0))
                    occ = task.get("occurrence_count", 0)
                    lines.append(
                        f"{index}. \U0001f501 [{task_id}] {content} | {interval_desc} | 创建者: {creator_name} | 下次: {remind_at} | 已触发{occ}次"
                    )
                else:
                    lines.append(f"{index}. [{task_id}] {content} | 创建者: {creator_name} | {remind_at} | {status_text}")

        if not lines:
            lines.append("当前查询者无可见任务（群聊仅展示本人创建的任务）")

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
        """
        在框架启动时初始化提醒调度器。
        
        Args:
            message: 事件消息（此事件中为 None）
            
        Returns:
            Tuple: (成功, 继续执行, 日志消息, None, None)
        """
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
        """
        在框架停止时关闭提醒调度器。
        
        Args:
            message: 事件消息（此事件中为 None）
            
        Returns:
            Tuple: (成功, 继续执行, 日志消息, None, None)
        """
        global _plugin_instance

        if _plugin_instance is None:
            return True, True, "插件实例为空，跳过停止", None, None

        await _plugin_instance.stop_scheduler()
        return True, True, "Mai_Plan 调度器已停止", None, None


@register_plugin
class MaiPlanPlugin(BasePlugin):
    """Mai_Plan 插件：智能日程管家，自动识别聊天待办并准时提醒"""

    plugin_name = "mai_plan_plugin"
    enable_plugin = True
    dependencies: List[str] = []
    python_dependencies: List[str] = ["apscheduler>=3.10,<4"]
    config_file_name = "config.toml"

    config_section_descriptions = {
        "plugin": "插件基础配置",
        "scope": "会话范围配置",
        "time": "时间格式配置",
        "reminder": "提醒文案与发送策略",
        "storage": "任务存储配置",
        "permission": "权限配置",
        "recurring": "循环任务配置",
    }

    config_schema: Dict[str, Dict[str, ConfigField]] = {
        "plugin": {
            "enabled": ConfigField(type=bool, default=True, description="是否启用插件"),
            "config_version": ConfigField(type=str, default="0.2.0", description="配置文件版本"),
        },
        "scope": {
            "group": ConfigField(type=bool, default=True, description="是否在群聊中生效"),
            "private": ConfigField(type=bool, default=True, description="是否在私聊中生效"),
        },
        # 统一使用 DEFAULT_TIME_FORMAT，废弃配置项
        # "time": {
        #     "format": ConfigField(type=str, default=DEFAULT_TIME_FORMAT, description="任务时间格式"), 
        # },
        "reminder": {
            "send_mode": ConfigField(
                type=str,
                default="origin_chat",
                description="提醒发送模式",
                choices=["origin_chat", "private_first"],
            ),
            # 废弃配置项
            #"prefix": ConfigField(type=str, default="注意下述提醒内容", description="提醒消息前缀"),
        },
        # 统一使用 plan_tasks.json，废弃配置项
        # "storage": {
        #     "tasks_file_name": ConfigField(type=str, default="plan_tasks.json", description="任务文件名"), 
        # },
        "permission": {
            "admin_user_ids": ConfigField(type=list, default=[], description="管理员用户 ID 列表"),
        },
        "recurring": {
            "max_recurring_per_chat": ConfigField(type=int, default=10, description="每个会话最大循环/间隔任务数"),
            "min_interval_seconds": ConfigField(type=int, default=60, description="间隔任务最小间隔秒数"),
            "max_interval_seconds": ConfigField(type=int, default=604800, description="间隔任务最大间隔秒数（默认7天）"),
        },
        "Others": {
            "save_plan_history": ConfigField(type=bool, default=True, description="任务完成后是否将记录归档到 plan_history.json（否则直接删除）"),
        },
    }

    def __init__(self, *args, **kwargs):
        """
        初始化插件实例。
        创建任务读写锁和 APScheduler 调度器引用。
        """
        global _plugin_instance

        super().__init__(*args, **kwargs)
        self._tasks_lock = asyncio.Lock()
        self._scheduler: Optional[AsyncIOScheduler] = None
        _plugin_instance = self

    def get_plugin_components(self) -> List[Tuple[ComponentInfo, Type]]:
        """
        获取插件暴露的所有组件（Action、Command、Handler）。
        
        Returns:
            List[Tuple[ComponentInfo, Type]]: 组件元信息与类型的列表
        """
        return [
            (MaiPlanCommand.get_command_info(), MaiPlanCommand),
            (MaiPlanStartupHandler.get_handler_info(), MaiPlanStartupHandler),
            (MaiPlanStopHandler.get_handler_info(), MaiPlanStopHandler),
            (CreatePlanTaskTool.get_tool_info(), CreatePlanTaskTool),
            (DeletePlanTaskTool.get_tool_info(), DeletePlanTaskTool),
            (ModifyPlanTaskTool.get_tool_info(), ModifyPlanTaskTool),
            (ListPlanTasksTool.get_tool_info(), ListPlanTasksTool),
        ]

    @staticmethod
    def status_to_text(status: str) -> str:
        """
        将任务状态码转换为人类可读的中文描述。
        
        Args:
            status: 状态码（pending、sent、failed、cancelled）
            
        Returns:
            str: 状态描述文本
        """
        status_map = {
            TASK_STATUS_PENDING: "待提醒",
            TASK_STATUS_SENT: "已提醒",
            TASK_STATUS_FAILED: "失败",
            TASK_STATUS_CANCELLED: "已取消",
        }
        return status_map.get(status, status or "未知")

    @staticmethod
    def _safe_int(value: Any, default: int, minimum: int = 0) -> int:
        """
        安全地将值转换为整数，捕获转换异常并返回默认值。
        
        Args:
            value: 待转换值
            default: 转换失败时的默认值
            minimum: 返回值的最小值
            
        Returns:
            int: 转换后的整数（不低于 minimum）
        """
        try:
            parsed = int(value)
        except (TypeError, ValueError):
            parsed = default
        return max(minimum, parsed)

    def _get_time_format(self) -> str:
        """
        获取配置中的时间格式字符串。
        若配置为空或无效，使用默认时间格式。
        
        Returns:
            str: 时间格式字符串
        """
        #time_format = self.get_config("time.format", DEFAULT_TIME_FORMAT) # 时间格式配置项已废弃，统一使用 DEFAULT_TIME_FORMAT
        time_format = DEFAULT_TIME_FORMAT
        if not isinstance(time_format, str) or not time_format.strip():
            return DEFAULT_TIME_FORMAT
        return time_format.strip()

    def _format_now(self) -> str:
        """
        获取当前时间的格式化字符串。
        
        Returns:
            str: 当前时间（格式 YYYY-MM-DD HH:MM:SS）
        """
        return datetime.now().strftime(DEFAULT_TIME_FORMAT)

    def _get_tasks_file_path(self) -> Path:
        """
        获取任务文件的完整路径。
        
        Returns:
            Path: 任务文件路径
        """
        #file_name = self.get_config("storage.tasks_file_name", "plan_tasks.json") # 任务文件名配置项已废弃，统一使用 plan_tasks.json
        #if not isinstance(file_name, str) or not file_name.strip():
        file_name = "plan_tasks.json"
        safe_name = Path(file_name.strip()).name
        return Path(self.plugin_dir) / safe_name

    def _empty_tasks_document(self) -> Dict[str, Any]:
        """
        创建并返回空的任务文档结构。
        
        Returns:
            Dict[str, Any]: 包含版本和空任务列表的字典
        """
        return {"version": TASK_FILE_VERSION, "tasks": []}

    def _normalize_tasks_document(self, data: Any) -> Dict[str, Any]:
        """
        规范化任务文档，过滤无效数据并确保结构一致。
        
        Args:
            data: 待规范化的数据（任意类型）
            
        Returns:
            Dict[str, Any]: 规范化后的任务文档
        """
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
        """
        读取任务文档（不加锁版本，需外部同步调用）。
        若读取失败，自动备份损坏文件并返回空文档。
        
        Returns:
            Dict[str, Any]: 任务文档
        """
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
        """
        写入任务文档（不加锁版本，需外部同步调用）。
        使用临时文件和原子操作保证数据完整性。
        
        Args:
            document: 待写入的任务文档
        """
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

    def _get_history_file_path(self) -> Path:
        """
        获取历史任务文件 plan_history.json 的完整路径。

        Returns:
            Path: 历史文件路径
        """
        return Path(self.plugin_dir) / "plan_history.json"

    def _read_history_unlocked(self) -> List[Dict[str, Any]]:
        """
        读取历史任务列表（不加锁，需外部同步调用）。
        若文件不存在或损坏，返回空列表。

        Returns:
            List[Dict[str, Any]]: 历史任务列表
        """
        history_file = self._get_history_file_path()
        if not history_file.exists():
            return []

        try:
            with history_file.open("r", encoding="utf-8") as file:
                data = json.load(file)
            if isinstance(data, list):
                return [item for item in data if isinstance(item, dict)]
            return []
        except Exception as error:
            logger.error(f"[Mai_Plan] 读取历史文件失败：{error}")
            return []

    def _write_history_unlocked(self, history: List[Dict[str, Any]]) -> None:
        """
        写入历史任务列表（不加锁，需外部同步调用）。
        使用临时文件和原子操作保证数据完整性。

        Args:
            history: 历史任务列表
        """
        history_file = self._get_history_file_path()
        history_file.parent.mkdir(parents=True, exist_ok=True)
        tmp_file = history_file.with_suffix(".json.tmp")

        try:
            with tmp_file.open("w", encoding="utf-8") as file:
                json.dump(history, file, ensure_ascii=False, indent=2)
            os.replace(tmp_file, history_file)
        finally:
            if tmp_file.exists():
                with contextlib.suppress(Exception):
                    tmp_file.unlink()

    def _archive_task_to_history(self, task: Dict[str, Any]) -> None:
        """
        将已完成的任务归档到 plan_history.json。
        需在持有 _tasks_lock 时调用。

        Args:
            task: 待归档的任务字典
        """
        history = self._read_history_unlocked()
        archived = dict(task)
        archived["archived_at"] = self._format_now()
        history.append(archived)
        self._write_history_unlocked(history)

    # ──────────────────────────────────────────────────────────
    #  Cron 关键字参数 辅助方法
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _validate_cron_kwargs(kwargs: Dict[str, Any]) -> Tuple[bool, str]:
        """
        校验 CronTrigger 关键字参数是否合法。
        利用 APScheduler 的 CronTrigger(**kwargs) 进行实例化校验。

        Args:
            kwargs: CronTrigger 关键字参数字典

        Returns:
            Tuple[bool, str]: (是否合法, 错误信息（合法时为空字符串）)
        """
        if not kwargs or not isinstance(kwargs, dict):
            return False, "cron 参数不能为空"

        # 过滤非法 key
        invalid_keys = set(kwargs.keys()) - CRON_KWARGS_ALLOWED_KEYS
        if invalid_keys:
            return False, f"cron 参数包含非法字段：{', '.join(sorted(invalid_keys))}。允许的字段：{', '.join(sorted(CRON_KWARGS_ALLOWED_KEYS))}"

        try:
            CronTrigger(**{k: v for k, v in kwargs.items() if k in CRON_KWARGS_ALLOWED_KEYS})
            return True, ""
        except (ValueError, KeyError, TypeError) as e:
            return False, f"cron 参数无效：{e}"

    @staticmethod
    def _build_cron_trigger_from_kwargs(cron_kwargs: Dict[str, Any]) -> CronTrigger:
        """
        根据关键字参数字典构建 CronTrigger 实例。

        Args:
            cron_kwargs: CronTrigger 关键字参数

        Returns:
            CronTrigger: APScheduler 触发器实例

        Raises:
            ValueError: 参数非法
        """
        filtered = {k: v for k, v in cron_kwargs.items() if k in CRON_KWARGS_ALLOWED_KEYS}
        return CronTrigger(**filtered)

    @staticmethod
    def _describe_cron_kwargs(kwargs: Dict[str, Any]) -> str:
        """
        将 CronTrigger 关键字参数翻译为简洁中文描述。

        Args:
            kwargs: CronTrigger 关键字参数字典

        Returns:
            str: 人类可读的中文描述
        """
        if not kwargs or not isinstance(kwargs, dict):
            return "未知"

        weekday_names = {
            "mon": "一", "tue": "二", "wed": "三", "thu": "四",
            "fri": "五", "sat": "六", "sun": "日",
            "0": "一", "1": "二", "2": "三", "3": "四",
            "4": "五", "5": "六", "6": "日",
        }

        parts: List[str] = []

        try:
            minute = kwargs.get("minute")
            hour = kwargs.get("hour")
            day = kwargs.get("day")
            month = kwargs.get("month")
            dow = kwargs.get("day_of_week")

            # 步进模式
            m_step = re.match(r"^\*/(\d+)$", str(minute)) if minute is not None else None
            h_step = re.match(r"^\*/(\d+)$", str(hour)) if hour is not None else None

            if m_step and hour is None and day is None and month is None and dow is None:
                return f"每 {m_step.group(1)} 分钟"

            if h_step and day is None and month is None and dow is None:
                n = h_step.group(1)
                if minute is None or str(minute) == "0":
                    return f"每 {n} 小时整点"
                return f"每 {n} 小时的第 {minute} 分"

            # 周期前缀
            if month is not None and day is not None:
                parts.append(f"每年 {month} 月 {day} 日")
            elif day is not None:
                if str(day) == "last":
                    parts.append("每月最后一天")
                else:
                    parts.append(f"每月 {day} 日")
            elif dow is not None:
                dow_str = str(dow).lower()
                if dow_str in ("mon-fri", "1-5"):
                    parts.append("工作日")
                elif dow_str in ("sat,sun", "sun,sat", "0,6", "6,0", "5,6"):
                    parts.append("周末")
                else:
                    names = []
                    for seg in dow_str.split(","):
                        seg = seg.strip()
                        name = weekday_names.get(seg)
                        if name:
                            names.append(name)
                        else:
                            names.append(seg)
                    parts.append("每周" + ",".join(names))
            else:
                if month is None and day is None and dow is None:
                    parts.append("每天")

            # 时间部分
            if hour is not None and not h_step:
                h_str = str(hour)
                m_str = str(minute) if minute is not None else "0"
                try:
                    # 处理逗号分隔的多个小时
                    if "," in h_str:
                        hours = h_str.split(",")
                        times = ", ".join(f"{int(h):02d}:{int(m_str):02d}" for h in hours)
                        parts.append(times)
                    else:
                        parts.append(f"{int(h_str):02d}:{int(m_str):02d}")
                except (ValueError, TypeError):
                    parts.append(f"{h_str}:{m_str}")
            elif minute is not None and not m_step:
                parts.append(f"第 {minute} 分")

        except Exception:
            return f"cron({kwargs})"

        return " ".join(parts) if parts else f"cron({kwargs})"

    def _compute_first_cron_fire_time(self, cron_kwargs: Dict[str, Any]) -> Optional[datetime]:
        """
        根据 CronTrigger 关键字参数计算从当前时刻起的第一次触发时间。

        Args:
            cron_kwargs: CronTrigger 关键字参数字典

        Returns:
            Optional[datetime]: 下次触发的 datetime，若参数无效则返回 None
        """
        try:
            trigger = self._build_cron_trigger_from_kwargs(cron_kwargs)
            next_fire = trigger.get_next_fire_time(None, datetime.now())
            return next_fire
        except Exception:
            return None

    # ──────────────────────────────────────────────────────────
    #  Interval 辅助方法
    # ──────────────────────────────────────────────────────────

    @staticmethod
    def _parse_interval_expr(expr: str) -> Optional[int]:
        """
        解析人可读的间隔表达式为总秒数。
        支持格式：'30s'、'5m'、'2h'、'1d'、'1h30m'、'2h15m30s' 等组合。

        Args:
            expr: 间隔表达式字符串

        Returns:
            Optional[int]: 总秒数，无效时返回 None
        """
        if not expr or not isinstance(expr, str):
            return None
        expr = expr.strip().lower()
        if not expr:
            return None

        pattern = r'^(?:(\d+)d)?\s*(?:(\d+)h)?\s*(?:(\d+)m)?\s*(?:(\d+)s)?$'
        match = re.match(pattern, expr)
        if not match:
            return None

        days = int(match.group(1) or 0)
        hours = int(match.group(2) or 0)
        minutes = int(match.group(3) or 0)
        seconds = int(match.group(4) or 0)

        total = days * 86400 + hours * 3600 + minutes * 60 + seconds
        return total if total > 0 else None

    @staticmethod
    def _describe_interval(seconds: int) -> str:
        """
        将间隔秒数转换为人类可读的中文描述。

        Args:
            seconds: 间隔秒数

        Returns:
            str: 中文描述，如 "每 2 小时 30 分钟"
        """
        if seconds <= 0:
            return "未知间隔"

        parts: List[str] = []
        remaining = seconds

        if remaining >= 86400:
            d = remaining // 86400
            remaining %= 86400
            parts.append(f"{d} 天")
        if remaining >= 3600:
            h = remaining // 3600
            remaining %= 3600
            parts.append(f"{h} 小时")
        if remaining >= 60:
            m = remaining // 60
            remaining %= 60
            parts.append(f"{m} 分钟")
        if remaining > 0:
            parts.append(f"{remaining} 秒")

        return "每 " + " ".join(parts) if parts else "未知间隔"

    def _register_job(self, task: Dict[str, Any]) -> None:
        """
        为任务在 APScheduler 中注册 job。
        根据 schedule_type 选择触发器：
        - interval → IntervalTrigger（start_date 控制首次触发）
        - cron → CronTrigger（关键字参数）
        - once（默认）→ DateTrigger
        需在 self._scheduler 已启动时调用。

        Args:
            task: 任务字典
        """
        if not self._scheduler:
            return

        task_id = str(task.get("task_id", ""))
        if not task_id:
            return

        schedule_type = str(task.get("schedule_type", SCHEDULE_TYPE_ONCE))

        try:
            if schedule_type == SCHEDULE_TYPE_INTERVAL:
                interval_seconds = int(task.get("interval_seconds", 0))
                if interval_seconds <= 0:
                    logger.error(f"[Mai_Plan] interval 任务 {task_id} 的 interval_seconds 无效")
                    return
                remind_ts = float(task.get("remind_ts", 0.0))
                start_date = datetime.fromtimestamp(remind_ts) if remind_ts > 0 else None
                trigger = IntervalTrigger(seconds=interval_seconds, start_date=start_date)

            elif schedule_type == SCHEDULE_TYPE_CRON:
                cron_kwargs = task.get("cron_kwargs")
                if not cron_kwargs or not isinstance(cron_kwargs, dict):
                    logger.error(f"[Mai_Plan] cron 任务 {task_id} 的 cron_kwargs 无效")
                    return
                trigger = self._build_cron_trigger_from_kwargs(cron_kwargs)

            else:
                # once — 一次性任务
                remind_ts = float(task.get("remind_ts", 0.0))
                if remind_ts <= 0:
                    return
                run_date = datetime.fromtimestamp(remind_ts)
                trigger = DateTrigger(run_date=run_date)

            self._scheduler.add_job(
                self._on_task_due,
                trigger=trigger,
                id=task_id,
                args=[task_id],
                replace_existing=True,
                misfire_grace_time=120,
            )
        except Exception as error:
            logger.error(f"[Mai_Plan] 注册 job 失败（task_id={task_id}）：{error}")

    def _unregister_job(self, task_id: str) -> None:
        """
        从 APScheduler 中移除指定 job。安全调用，不抛异常。

        Args:
            task_id: 任务 ID
        """
        if self._scheduler:
            with contextlib.suppress(Exception):
                self._scheduler.remove_job(task_id)

    async def _on_task_due(self, task_id: str) -> None:
        """
        APScheduler 在任务触发时调用此回调。
        处理发送提醒、更新任务状态和循环任务的下次调度。

        Args:
            task_id: 触发的任务 ID
        """
        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            tasks = document["tasks"]

            target_task: Optional[Dict[str, Any]] = None
            for task in tasks:
                if str(task.get("task_id", "")) == task_id:
                    target_task = task
                    break

            if not target_task:
                logger.debug(f"[Mai_Plan] _on_task_due: 未找到任务 {task_id}，可能已被取消")
                return

            if str(target_task.get("status", "")) != TASK_STATUS_PENDING:
                logger.debug(f"[Mai_Plan] _on_task_due: 任务 {task_id} 状态非 pending，跳过")
                return

        # 在锁外执行发送（可能耗时）
        success, error_text = await self._send_due_task(target_task)
        update_time = self._format_now()

        async with self._tasks_lock:
            latest_document = self._read_tasks_document_unlocked()
            latest_tasks = latest_document["tasks"]

            live_task: Optional[Dict[str, Any]] = None
            for task in latest_tasks:
                if str(task.get("task_id", "")) == task_id:
                    live_task = task
                    break

            if not live_task:
                return
            if str(live_task.get("status", "")) != TASK_STATUS_PENDING:
                return

            is_repeating = live_task.get("schedule_type", "") in (SCHEDULE_TYPE_CRON, SCHEDULE_TYPE_INTERVAL)
            live_task["last_attempt_at"] = update_time

            if success:
                if is_repeating:
                    # 循环/间隔任务: 归档本次触发，更新下次时间，保持 pending
                    occurrence = self._safe_int(live_task.get("occurrence_count", 0), 0, 0)
                    occurrence += 1
                    live_task["occurrence_count"] = occurrence
                    live_task["triggered_at"] = update_time
                    live_task["retry_count"] = 0
                    live_task["last_error"] = ""

                    # 归档本次触发记录
                    if self.get_config("Others.save_plan_history", True):
                        archive_copy = dict(live_task)
                        archive_copy["occurrence_index"] = occurrence
                        self._archive_task_to_history(archive_copy)

                    # 从 scheduler 获取下次触发时间
                    next_run = None
                    if self._scheduler:
                        job = self._scheduler.get_job(task_id)
                        if job and job.next_run_time:
                            next_run = job.next_run_time
                    if next_run:
                        live_task["remind_at"] = next_run.strftime(DEFAULT_TIME_FORMAT)
                        live_task["remind_ts"] = next_run.timestamp()
                    # 保持 status = pending
                else:
                    # 一次性任务: 归档 + 从文件删除
                    live_task["status"] = TASK_STATUS_SENT
                    live_task["triggered_at"] = update_time
                    live_task["last_error"] = ""

                    if self.get_config("Others.save_plan_history", True):
                        self._archive_task_to_history(live_task)

                    latest_document["tasks"] = [
                        t for t in latest_tasks
                        if str(t.get("task_id", "")) != task_id
                    ]
            else:
                current_retry = self._safe_int(live_task.get("retry_count", 0), 0, 0)
                current_retry += 1
                live_task["retry_count"] = current_retry
                live_task["last_error"] = error_text

                max_retry = self._safe_int(self.get_config("scheduler.max_retry_count", 3), 3, 0)
                if current_retry > max_retry:
                    live_task["status"] = TASK_STATUS_FAILED
                    # 循环/间隔任务失败时从 scheduler 移除
                    if is_repeating:
                        self._unregister_job(task_id)

            self._write_tasks_document_unlocked(latest_document)

    def _parse_remind_datetime(self, remind_time_str: str) -> Tuple[Optional[datetime], str]:
        """
        解析提醒时间字符串为 datetime 对象。
        
        Args:
            remind_time_str: 时间字符串
            
        Returns:
            Tuple[Optional[datetime], str]: (解析结果或 None, 错误信息（无错误时为空字符串）)
        """
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
        """
        生成唯一的任务 ID。
        格式：p_hex6
        
        Args:
            existing_ids: 已存在的任务 ID 集合（用于避免重复）
            
        Returns:
            str: 新生成的任务 ID
        """
        for _ in range(10):
            task_id = f"p_{uuid.uuid4().hex[:6]}"
            if task_id not in existing_ids:
                return task_id
        return f"p_{uuid.uuid4().hex[:12]}"

    def _resolve_target_stream_id(self, task: Dict[str, Any]) -> str:
        """
        确定提醒消息的目标发送流。
        根据 send_mode 配置选择原会话或用户私聊。
        
        Args:
            task: 任务字典
            
        Returns:
            str: 目标 stream_id
        """
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
        """
        检查任务功能在当前会话类型（群聊/私聊）是否启用。
        
        Args:
            is_group: 是否为群聊
            
        Returns:
            bool: 是否启用
        """
        if is_group:
            return bool(self.get_config("scope.group", True))
        return bool(self.get_config("scope.private", True))

    def is_admin(self, user_id: str) -> bool:
        """
        检查用户是否为管理员。
        
        Args:
            user_id: 用户 ID
            
        Returns:
            bool: 是否为管理员
        """
        admin_ids = self.get_config("permission.admin_user_ids", [])
        if not isinstance(admin_ids, list):
            return False
        normalized = {str(item).strip() for item in admin_ids if str(item).strip()}
        return str(user_id).strip() in normalized

    def extract_task_content(self, action_data: Dict[str, Any]) -> str:
        """
        从动作数据中提取任务内容。
        尝试多个可能的字段名。
        
        Args:
            action_data: 动作数据字典
            
        Returns:
            str: 任务内容（空字符串表示未找到）
        """
        keys = ["task_content", "content", "task", "todo", "plan_content", "reminder_content"]
        for key in keys:
            value = action_data.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        return ""

    def extract_remind_time(self, action_data: Dict[str, Any]) -> str:
        """
        从动作数据中提取提醒时间。
        尝试多个可能的字段名。
        
        Args:
            action_data: 动作数据字典
            
        Returns:
            str: 提醒时间字符串（空字符串表示未找到）
        """
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
        schedule_type: str = SCHEDULE_TYPE_ONCE,
        cron_kwargs: Optional[Dict[str, Any]] = None,
        interval_expr: Optional[str] = None,
    ) -> Tuple[bool, str, Optional[Dict[str, Any]]]:
        """
        创建一个新的计划任务并持久化。
        支持一次性任务、cron 循环任务和 interval 间隔任务。

        Args:
            chat_id: 会话 ID
            creator_user_id: 创建者用户 ID
            creator_name: 创建者用户名
            content: 任务内容
            remind_time_str: 提醒时间字符串（一次性任务必填，循环/间隔任务可留空）
            source_message_id: 源消息 ID（可选）
            platform: 平台名（可选）
            is_group: 是否为群聊
            schedule_type: 调度类型 ("once" / "cron" / "interval")
            cron_kwargs: CronTrigger 关键字参数字典（cron 必填）
            interval_expr: 间隔表达式如 '30m', '2h'（interval 必填）

        Returns:
            Tuple[bool, str, Optional[Dict]]: (成功标志, 回复文本, 任务字典或 None)
        """
        if not chat_id:
            return False, "创建任务失败：缺少 chat_id", None

        content = content.strip()
        if not content:
            return False, "创建任务失败：任务内容不能为空, 请要求用户提供具体的提醒内容", None

        # ── 初始化间隔相关变量 ──
        interval_seconds: Optional[int] = None

        # ── 调度类型校验 ──
        if schedule_type == SCHEDULE_TYPE_CRON:
            if not cron_kwargs:
                return False, "创建循环任务失败：缺少 cron_kwargs", None

            valid, cron_error = self._validate_cron_kwargs(cron_kwargs)
            if not valid:
                return False, f"创建循环任务失败：{cron_error}", None

            first_fire = self._compute_first_cron_fire_time(cron_kwargs)
            if first_fire is None:
                return False, "创建循环任务失败：无法计算首次触发时间", None

            remind_dt = first_fire
            remind_timestamp = remind_dt.timestamp()
            remind_at = remind_dt.strftime(DEFAULT_TIME_FORMAT)

        elif schedule_type == SCHEDULE_TYPE_INTERVAL:
            if not interval_expr:
                return False, "创建间隔任务失败：缺少 interval_expr", None

            parsed = self._parse_interval_expr(interval_expr)
            if parsed is None or parsed <= 0:
                return False, f"创建间隔任务失败：无法解析间隔表达式 '{interval_expr}'", None
            interval_seconds = parsed

            min_iv = self._safe_int(self.get_config("recurring.min_interval_seconds", 60), 60, 1)
            max_iv = self._safe_int(self.get_config("recurring.max_interval_seconds", 604800), 604800, 60)
            if interval_seconds < min_iv:
                return False, f"创建间隔任务失败：间隔不能小于 {min_iv} 秒", None
            if interval_seconds > max_iv:
                return False, f"创建间隔任务失败：间隔不能大于 {max_iv} 秒", None

            remind_dt = datetime.now() + timedelta(seconds=interval_seconds)
            remind_timestamp = remind_dt.timestamp()
            remind_at = remind_dt.strftime(DEFAULT_TIME_FORMAT)

        else:
            # ── 一次性任务校验 ──
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

            # 循环/间隔任务数量限制检查
            if schedule_type in (SCHEDULE_TYPE_CRON, SCHEDULE_TYPE_INTERVAL):
                max_recurring = self._safe_int(
                    self.get_config("recurring.max_recurring_per_chat", 10), 10, 1
                )
                current_recurring = sum(
                    1 for t in tasks
                    if str(t.get("chat_id", "")) == chat_id
                    and t.get("schedule_type", "") in (SCHEDULE_TYPE_CRON, SCHEDULE_TYPE_INTERVAL)
                    and str(t.get("status", "")) == TASK_STATUS_PENDING
                )
                if current_recurring >= max_recurring:
                    return False, f"创建失败：当前会话循环/间隔任务数已达上限（{max_recurring}）", None

            type_label_map = {
                SCHEDULE_TYPE_CRON: "（循环任务）",
                SCHEDULE_TYPE_INTERVAL: "（间隔任务）",
                SCHEDULE_TYPE_ONCE: "（一次性任务）",
            }

            task_type_hint = type_label_map.get(schedule_type, "（一次性任务）")
            llm_candidate_tasks = [
                task for task in tasks
                if str(task.get("chat_id", "")) == chat_id
                and str(task.get("status", "")) == TASK_STATUS_PENDING
                and str(task.get("schedule_type", SCHEDULE_TYPE_ONCE) or SCHEDULE_TYPE_ONCE).lower() == schedule_type
            ]

            if llm_candidate_tasks:
                check_success, check_message, reasoning, model_name = await llm_api.generate_with_model(
                    "你是任务去重判定器，目标是降低误判（宁可漏判，不可错判）。\n"
                    f"待创建任务：content=「{content}」{task_type_hint}，chat_id={chat_id}，schedule_type={schedule_type}，"
                    f"remind_at={remind_at}，cron_kwargs={cron_kwargs if schedule_type == SCHEDULE_TYPE_CRON else None}，"
                    f"interval_seconds={interval_seconds if schedule_type == SCHEDULE_TYPE_INTERVAL else None}。\n"
                    "请在“待处理任务列表”中只返回与待创建任务“确定重复”的 task_id。\n"
                    "重复判定硬条件（必须全部满足）：\n"
                    "1) chat_id 相同；\n"
                    "2) status 必须为 pending；\n"
                    "3) schedule_type 相同；\n"
                    "4) content 核心语义相同（动作+对象一致；仅主题相近不算重复）；\n"
                    "5) 分类型附加条件：once 要求 remind_at 相同；cron 要求 cron_kwargs 等价；interval 要求 interval_seconds 相同。\n"
                    "输出规则：\n"
                    "1) 只输出 task_id，多个用单个空格分隔；\n"
                    "2) 不输出解释、前后缀、标点、换行或代码块；\n"
                    "3) 若无法确定重复，返回None；\n"
                    f"待处理任务列表：{llm_candidate_tasks}",
                    model_config.model_task_config.tool_use,
                    request_type="mai_only_you",
                )

                if check_success and check_message:
                    returned_ids = set(re.findall(r"\bp_[0-9a-fA-F]{6,12}\b", check_message.strip()))
                    if not returned_ids:
                        returned_ids = {check_message.strip()}
                    for task in llm_candidate_tasks:
                        if str(task.get("task_id", "")) in returned_ids:
                            return False, f"创建失败：相同提醒已存在（任务ID：{task.get('task_id', '-')})", task

            # 精确去重（仅一次性任务）
            # if not is_recurring:
            #     for task in tasks:
            #         if str(task.get("chat_id", "")) != chat_id:
            #             continue
            #         if str(task.get("content", "")).strip() != content:
            #             continue
            #         if str(task.get("remind_at", "")) != remind_at:
            #             continue
            #         task_status = str(task.get("status", ""))
            #         if task_status in {TASK_STATUS_PENDING, TASK_STATUS_SENT}:
            #             task_id = str(task.get("task_id", "-"))
            #             return False, f"创建失败：相同提醒已存在（任务ID：{task_id}）", task

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
                "schedule_type": schedule_type,
                "cron_kwargs": cron_kwargs if schedule_type == SCHEDULE_TYPE_CRON else None,
                "interval_seconds": interval_seconds if schedule_type == SCHEDULE_TYPE_INTERVAL else None,
                "interval_expr": interval_expr if schedule_type == SCHEDULE_TYPE_INTERVAL else None,
                "occurrence_count": 0,
            }

            tasks.append(task)
            tasks.sort(key=lambda item: float(item.get("remind_ts", 0.0)))
            self._write_tasks_document_unlocked(document)

        # 注册到 APScheduler
        self._register_job(task)

        if schedule_type == SCHEDULE_TYPE_CRON:
            cron_desc = self._describe_cron_kwargs(cron_kwargs or {})
            reply_text = "\n".join(
                [
                    f"已创建cron循环任务：{content}",
                    f"循环规则：{cron_desc}",
                    f"首次提醒：{remind_at}",
                ]
            )
        elif schedule_type == SCHEDULE_TYPE_INTERVAL:
            interval_desc = self._describe_interval(interval_seconds or 0)
            reply_text = "\n".join(
                [
                    f"已创建间隔任务：{content}",
                    f"间隔规则：{interval_desc}",
                    f"首次提醒：{remind_at}",
                ]
            )
        else:
            reply_text = "\n".join(
                [
                    f"已创建计划任务：{content}",
                    f"提醒时间：{remind_at}",
                ]
            )
        return True, reply_text, task

    async def list_tasks(self, chat_id: str, include_all_tasks: bool = False, creator_name: str = None) -> List[Dict[str, Any]]:
        """
        列出任务。
        
        Args:
            chat_id: 会话 ID
            include_all_tasks: 是否返回 plan_tasks.json 中的全部任务
            creator_name: 任务创建者名称（可选，用于筛选特定创建者创建的任务）
            
        Returns:
            List[Dict[str, Any]]: 任务列表（按提醒时间排序）
        """
        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()

        if include_all_tasks:
            tasks = list(document["tasks"])
        else:
            tasks = [task for task in document["tasks"] if str(task.get("chat_id", "")) == chat_id]
            if creator_name is not None:
                tasks = [task for task in tasks if str(task.get("creator_name", "")) == creator_name]
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
        """
        取消一个任务。
        权限检查：只有任务创建者或管理员可以取消。
        
        Args:
            chat_id: 会话 ID
            task_id: 任务 ID
            operator_user_id: 操作者用户 ID
            is_admin: 操作者是否为管理员
            
        Returns:
            Tuple[bool, str]: (成功标志, 结果信息)
        """
        if not task_id:
            return False, "取消失败：task_id 不能为空"

        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            tasks = document["tasks"]

            target_task: Optional[Dict[str, Any]] = None
            for task in tasks:
                #管理员拥有删除所有聊天流计划的权限，非管理员只能删除自己创建的计划
                if (is_admin or str(task.get("chat_id", "")) == chat_id) and str(task.get("task_id", "")) == task_id: 
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

            if self.get_config("Others.save_plan_history", True):
                self._archive_task_to_history(target_task)

            document["tasks"] = [
                t for t in tasks
                if str(t.get("task_id", "")) != task_id
            ]
            self._write_tasks_document_unlocked(document)

        # 从 APScheduler 移除 job
        self._unregister_job(task_id)
        return True, f"已取消任务 {task_id}"

    async def update_task(
        self,
        chat_id: str,
        task_id: str,
        new_content: Optional[str] = None,
        new_remind_time_str: Optional[str] = None,
        new_cron_kwargs: Optional[Dict[str, Any]] = None,
        new_interval_expr: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        修改一个待处理任务的内容、提醒时间、cron 关键字参数或间隔表达式。
        仅允许修改状态为 pending 的任务。

        Args:
            chat_id: 会话 ID
            task_id: 任务 ID
            new_content: 新的任务内容（None 表示不修改）
            new_remind_time_str: 新的提醒时间字符串（None 表示不修改）
            new_cron_kwargs: 新的 CronTrigger 关键字参数（None 表示不修改，仅 cron 任务可用）
            new_interval_expr: 新的间隔表达式（None 表示不修改，仅 interval 任务可用）

        Returns:
            Tuple[bool, str]: (成功标志, 结果信息)
        """
        if not task_id:
            return False, "修改失败：task_id 不能为空"

        if not new_content and not new_remind_time_str and not new_cron_kwargs and not new_interval_expr:
            return False, "修改失败：至少需要提供新的任务内容、提醒时间、cron 参数或间隔表达式"

        # 若提供了新时间，先在锁外完成解析校验
        new_remind_dt: Optional[datetime] = None
        if new_remind_time_str:
            new_remind_dt, parse_error = self._parse_remind_datetime(new_remind_time_str)
            if new_remind_dt is None:
                return False, f"修改失败：{parse_error}"

            min_future_seconds = self._safe_int(
                self.get_config("scheduler.min_future_seconds", 30), 30, 0
            )
            if new_remind_dt.timestamp() < time.time() + min_future_seconds:
                return False, f"修改失败：新的提醒时间必须晚于当前时间至少 {min_future_seconds} 秒"

        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            tasks = document["tasks"]

            target_task: Optional[Dict[str, Any]] = None
            for task in tasks:
                if str(task.get("chat_id", "")) == chat_id and str(task.get("task_id", "")) == task_id:
                    target_task = task
                    break

            if target_task is None:
                return False, f"修改失败：未找到任务 {task_id}"

            status = str(target_task.get("status", ""))
            if status != TASK_STATUS_PENDING:
                status_text = self.status_to_text(status)
                return False, f"修改失败：任务 {task_id} 当前状态为「{status_text}」，仅待提醒任务可修改"

            changes: List[str] = []
            old_content = str(target_task.get("content", ""))
            old_remind_at = str(target_task.get("remind_at", ""))
            stype = target_task.get("schedule_type", SCHEDULE_TYPE_ONCE)

            if new_content:
                target_task["content"] = new_content
                changes.append(f"内容：{old_content} → {new_content}")

            if new_remind_dt is not None:
                new_remind_at = new_remind_dt.strftime(DEFAULT_TIME_FORMAT)
                target_task["remind_at"] = new_remind_at
                target_task["remind_ts"] = new_remind_dt.timestamp()
                changes.append(f"提醒时间：{old_remind_at} → {new_remind_at}")

            # 更新 cron 关键字参数（仅 cron 任务）
            if new_cron_kwargs:
                if stype != SCHEDULE_TYPE_CRON:
                    return False, "修改失败：仅 cron 循环任务可修改 cron 参数"
                valid, cron_error = self._validate_cron_kwargs(new_cron_kwargs)
                if not valid:
                    return False, f"修改失败：{cron_error}"
                old_cron_desc = self._describe_cron_kwargs(target_task.get("cron_kwargs", {}))
                target_task["cron_kwargs"] = new_cron_kwargs
                # 重新计算下次触发时间
                next_fire = self._compute_first_cron_fire_time(new_cron_kwargs)
                if next_fire:
                    target_task["remind_at"] = next_fire.strftime(DEFAULT_TIME_FORMAT)
                    target_task["remind_ts"] = next_fire.timestamp()
                new_cron_desc = self._describe_cron_kwargs(new_cron_kwargs)
                changes.append(f"cron：{old_cron_desc} → {new_cron_desc}")

            # 更新间隔表达式（仅 interval 任务）
            if new_interval_expr:
                if stype != SCHEDULE_TYPE_INTERVAL:
                    return False, "修改失败：仅间隔任务可修改间隔表达式"
                parsed = self._parse_interval_expr(new_interval_expr)
                if parsed is None or parsed <= 0:
                    return False, f"修改失败：无法解析间隔表达式 '{new_interval_expr}'"
                min_iv = self._safe_int(self.get_config("recurring.min_interval_seconds", 60), 60, 1)
                max_iv = self._safe_int(self.get_config("recurring.max_interval_seconds", 604800), 604800, 60)
                if parsed < min_iv:
                    return False, f"修改失败：间隔不能小于 {min_iv} 秒"
                if parsed > max_iv:
                    return False, f"修改失败：间隔不能大于 {max_iv} 秒"
                old_interval_desc = self._describe_interval(target_task.get("interval_seconds", 0))
                target_task["interval_seconds"] = parsed
                target_task["interval_expr"] = new_interval_expr
                # 重新计算下次触发时间
                new_fire = datetime.now() + timedelta(seconds=parsed)
                target_task["remind_at"] = new_fire.strftime(DEFAULT_TIME_FORMAT)
                target_task["remind_ts"] = new_fire.timestamp()
                new_interval_desc = self._describe_interval(parsed)
                changes.append(f"间隔：{old_interval_desc} → {new_interval_desc}")

            need_rejob = bool(new_remind_time_str or new_cron_kwargs or new_interval_expr)

            target_task["updated_at"] = self._format_now()

            # 重新按提醒时间排序
            tasks.sort(key=lambda item: float(item.get("remind_ts", 0.0)))
            self._write_tasks_document_unlocked(document)

        # 重新注册 APScheduler job
        if need_rejob:
            self._unregister_job(task_id)
            self._register_job(target_task)

        change_detail = "；".join(changes)
        return True, f"已修改任务 {task_id}：{change_detail}"

    async def start_scheduler(self) -> None:
        """
        启动 APScheduler 提醒调度器。
        从 JSON 文件恢复所有 pending 任务的 job 注册。
        对 interval 任务，重新计算首次触发时间 = 加载时刻 + interval_seconds。
        """
        if not bool(self.get_config("plugin.enabled", True)):
            logger.info("[Mai_Plan] 插件配置为禁用，跳过调度器启动")
            return

        if self._scheduler and self._scheduler.running:
            return

        self._scheduler = AsyncIOScheduler(misfire_grace_time=120)
        self._scheduler.start()

        load_time = datetime.now()

        # 从 JSON 恢复所有 pending 任务
        async with self._tasks_lock:
            document = self._read_tasks_document_unlocked()
            pending_tasks = [
                task for task in document["tasks"]
                if str(task.get("status", "")) == TASK_STATUS_PENDING
            ]

            # interval 任务重启恢复：重新计算首次触发时间
            dirty = False
            for task in pending_tasks:
                if task.get("schedule_type") == SCHEDULE_TYPE_INTERVAL:
                    iv_sec = task.get("interval_seconds")
                    if iv_sec and int(iv_sec) > 0:
                        new_fire = load_time + timedelta(seconds=int(iv_sec))
                        task["remind_ts"] = new_fire.timestamp()
                        task["remind_at"] = new_fire.strftime(DEFAULT_TIME_FORMAT)
                        dirty = True
                        logger.info(
                            f"[Mai_Plan] interval 任务 {task.get('task_id')} 重启恢复，"
                            f"下次触发: {task['remind_at']}"
                        )

            if dirty:
                document["tasks"].sort(key=lambda item: float(item.get("remind_ts", 0.0)))
                self._write_tasks_document_unlocked(document)

        registered_count = 0
        for task in pending_tasks:
            self._register_job(task)
            registered_count += 1

        logger.info(f"[Mai_Plan] APScheduler 调度器已启动，恢复 {registered_count} 个待处理任务")

    async def stop_scheduler(self) -> None:
        """
        停止 APScheduler 提醒调度器。
        """
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=False)
        self._scheduler = None
        logger.info("[Mai_Plan] APScheduler 调度器已停止")

    async def _send_due_task(self, task: Dict[str, Any]) -> Tuple[bool, str]:
        """
        发送一个到期任务的提醒。
        格式化提醒消息并通过 send_api 发送到指定会话。
        
        Args:
            task: 任务字典
            
        Returns:
            Tuple[bool, str]: (发送成功标志, 错误信息（成功时为空字符串）)
        """
        task_id = str(task.get("task_id", ""))
        chat_id = str(task.get("chat_id", ""))
        content = str(task.get("content", "")).strip()
        remind_at = str(task.get("remind_at", "")).strip()

        if not chat_id:
            return False, f"任务 {task_id} 缺少 chat_id"
        if not content:
            return False, f"任务 {task_id} 缺少 content"

#         prefix = self.get_config("reminder.prefix", "⏰ 日程提醒")
#         if not isinstance(prefix, str) or not prefix.strip():
#             prefix = "⏰ 日程提醒"

        target_stream_id = self._resolve_target_stream_id(task)
        if not target_stream_id:
            return False, f"任务 {task_id} 缺少可用 stream_id"

        message_text = "\n".join(
            [
                #str(prefix).strip(),
                f"任务：{content}",
                f"计划时间：{remind_at or '-'}",
            #    f"任务ID：{task_id}",
            ]
        )
        current_replyer = generator_api.get_replyer(chat_id = target_stream_id)

        generate_reply_success, llm_response = await generator_api.generate_reply(
            chat_id = target_stream_id, 
            reply_reason="现在需要执行你的回复计划或继续过去话题",
            extra_info = "请依据以下信息生成符合人设回复内容：\n" + message_text,
            )

        if generate_reply_success :
            success = await send_api.text_to_stream(
            text=llm_response.content,
            stream_id=target_stream_id,
            storage_message=True,
        )

        if not generate_reply_success:
            logger.error(f"[Mai_Plan] 生成提醒回复失败，使用默认消息内容，错误信息：{llm_response}")


        # success = await send_api.text_to_stream(
        #     text=message_text,
        #     stream_id=target_stream_id,
        #     storage_message=True,
        # )

        if success:
            return True, ""

        return False, f"任务 {task_id} 发送提醒失败（stream_id={target_stream_id}）"
