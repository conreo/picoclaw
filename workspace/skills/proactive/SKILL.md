---
name: proactive
description: "Native proactive agent capabilities for autonomous operations. Use when: (1) Setting up autonomous monitoring tasks, (2) Writing actions to WAL before execution for crash recovery, (3) Using working buffer for multi-step investigations, (4) Implementing self-healing behaviors, (5) Scheduling recurring autonomous tasks."
---

# Proactive Agent (Native Tool)

This is a **built-in native tool** in PicoClaw. Use the `proactive` tool directly - no installation needed.

Transform from reactive (wait → answer → wait) to proactive (monitor → anticipate → act → learn).

## Core Components

### 1. Write-Ahead Log (WAL)

Reliable state tracking with crash recovery. Every autonomous action is logged before execution.

**Location:** `{workspace}/memory/wal.json`

#### Actions

| Action | Description |
|--------|-------------|
| `wal_write` | Log an action before execution |
| `wal_complete` | Mark action as completed |
| `wal_fail` | Mark action as failed with recovery steps |
| `wal_read` | Read WAL entries (optionally filter by status) |
| `wal_recover` | Get pending entries for recovery |
| `wal_clear` | Clear completed entries |

#### Entry Types

- `monitor` - System checks, health monitoring
- `heal` - Self-healing actions
- `schedule` - Scheduled/recurring tasks
- `investigate` - Multi-step investigations

#### Example: Safe Autonomous Action

```
1. proactive action=wal_write type=monitor command="df -h" message="Check disk"
   → Returns: id=1234567890

2. [Execute the actual command]

3. proactive action=wal_complete entry_id=1234567890 output="Disk: 45% used"
```

#### Example: Recovery on Startup

```
proactive action=wal_recover
→ Returns list of pending entries that need completion/retry
```

---

### 2. Working Buffer

Temporary workspace for multi-step tasks that span multiple interactions.

**Location:** `{workspace}/memory/buffer.md`

#### Actions

| Action | Description |
|--------|-------------|
| `buffer_append` | Add content to buffer (optionally in a section) |
| `buffer_read` | Read buffer contents (optionally a specific section) |
| `buffer_clear` | Clear buffer or specific section |
| `buffer_sections` | List all sections |

#### Example: Investigation Buffer

```
proactive action=buffer_append section="evidence" content="Error found in /var/log/app.log at line 452"
proactive action=buffer_append section="evidence" content="Related to database connection timeout"
proactive action=buffer_read section="evidence"
proactive action=buffer_clear section="evidence"
```

---

## Integration with Cron

Combine `proactive` with the `cron` tool for autonomous scheduled tasks:

```
1. proactive action=wal_write type=schedule command="health check" message="Daily health check"
2. cron action=add cron_expr="0 9 * * *" message="Run daily health check" command="/path/to/check.sh"
3. [On completion] proactive action=wal_complete entry_id=...
```

---

## Self-Healing Pattern

```
1. proactive action=wal_write type=heal command="restart nginx" message="High error rate detected"
2. exec command="systemctl restart nginx"
3. [If success]
   proactive action=wal_complete entry_id=... output="Nginx restarted"
4. [If failure]
   proactive action=wal_fail entry_id=... output="Failed" recovery="Check nginx config, manual restart needed"
```

---

## File Locations

| Component | Path |
|-----------|------|
| WAL | `{workspace}/memory/wal.json` |
| Buffer | `{workspace}/memory/buffer.md` |

---

## Quick Reference

```
# WAL operations
proactive action=wal_write type=<monitor|heal|schedule|investigate> command="..." message="..."
proactive action=wal_complete entry_id=<id> output="..."
proactive action=wal_fail entry_id=<id> output="..." recovery="..."
proactive action=wal_read status=<pending|completed|failed>
proactive action=wal_recover

# Buffer operations
proactive action=buffer_append section="<name>" content="..."
proactive action=buffer_read section="<name>"
proactive action=buffer_clear section="<name>"
proactive action=buffer_sections
```
