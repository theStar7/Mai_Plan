# Mai_Plan 插件

面向 MaiBot 的日程提醒插件：从聊天内容自动创建计划任务，并在指定时间提醒。

## 功能
- 自动创建计划任务（由 Planner 触发 action）
- 定时提醒（支持失败重试）
- 任务管理命令：查看/取消
- 本地持久化任务数据

## 依赖与适配
- 适配 MaiBot `>= 0.12.1`
- 当前无额外 Python 依赖

## 使用方式
### 自动创建
当用户表达未来提醒诉求时，Planner 会调用 action `create_plan_task`，并传入参数：
- `task_content`: 任务内容
- `remind_time`: 绝对时间字符串，格式 `YYYY-MM-DD HH:MM:SS`

### 命令
- `/mai_plan list [pending|all]` 查看当前会话任务
- `/mai_plan cancel <task_id>` 取消任务

## 配置
配置文件为 [config.toml](config.toml)。常用配置如下：

- `scope.group` 是否在群聊生效
- `scope.private` 是否在私聊生效
- `time.format` 时间格式，默认 `"%Y-%m-%d %H:%M:%S"`
- `scheduler.tick_seconds` 扫描间隔（秒）
- `scheduler.min_future_seconds` 最小提前创建秒数
- `reminder.send_mode` 提醒发送模式：`origin_chat` / `private_first`
- `reminder.prefix` 提醒消息前缀
- `permission.admin_user_ids` 管理员用户 ID 列表

## 数据存储
任务文件默认存储为 [plan_tasks.json](plan_tasks.json)。结构示例：

```json
{
  "version": "1.0.0",
  "tasks": [
    {
      "task_id": "p_20260226_ab12cd34",
      "chat_id": "stream_xxx",
      "content": "明天提交周报",
      "remind_at": "2026-02-27 09:00:00",
      "status": "pending"
    }
  ]
}
```

状态说明：
- `pending` 待提醒
- `sent` 已提醒
- `failed` 失败
- `cancelled` 已取消

## 权限规则
- 任务创建者可取消自己的任务
- 管理员（`permission.admin_user_ids`）可取消任意任务

