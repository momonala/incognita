import logging
from datetime import datetime, timedelta

import requests

from incognita.values import TELEGRAM_CHAT_ID, TELEGRAM_TOKEN

logging.basicConfig(level=logging.INFO, format="%(asctime)s:%(levelname)s:%(name)s:%(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("werkzeug").setLevel(logging.WARNING)


# Daily summary tracking
daily_summary_message_id = None
daily_summary_date = None
heartbeat_events = []  # List of (timestamp, event_type, duration) tuples



def format_downtime(seconds: float) -> str:
    """Format seconds into human readable format based on duration."""
    td = timedelta(seconds=int(seconds))
    days = td.days
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    seconds = td.seconds % 60

    if days > 0:
        return f"{days}d, {hours}h, {minutes}m, {seconds}s"
    elif hours > 0:
        return f"{hours}h, {minutes}m, {seconds}s"
    else:
        return f"{minutes}m, {seconds}s"
    

def update_daily_summary(event_type: str, last_heartbeat: datetime, duration: int | None = None):
    """Update the daily summary message with heartbeat events."""
    global daily_summary_message_id, daily_summary_date, heartbeat_events
    
    now = datetime.now()
    today = now.date()
    
    # Check if we need to start a new daily summary
    if daily_summary_date != today:
        daily_summary_date = today
        daily_summary_message_id = None
        heartbeat_events = []
    
    # Add the new event
    if event_type == "lost":
        heartbeat_events.append((now, "lost", None))
    elif event_type == "recovered":
        # Add recovery as a separate event
        heartbeat_events.append((now, "recovered", duration))
    
    # Build the summary message
    summary_lines = [f"📊 Heartbeat Summary - {today.strftime('%Y-%m-%d')}"]
    
    for timestamp, event, duration_sec in heartbeat_events:
        time_str = timestamp.strftime("%H:%M:%S")
        if event == "lost":
            summary_lines.append(f"❌ {time_str} - Heartbeat lost")
        elif event == "recovered":
            duration_str = format_downtime(duration_sec) if duration_sec else "unknown"
            summary_lines.append(f"✅ {time_str} - Heartbeat recovered (downtime: {duration_str})")
    
    # Add current status
    current_downtime = int((now - last_heartbeat).total_seconds())
    if current_downtime < 60:
        summary_lines.append(f"💚 Current: Online (last heartbeat: {last_heartbeat.strftime('%H:%M:%S')})")
    else:
        summary_lines.append(f"🪦 Current: Offline for {format_downtime(current_downtime)}")
    
    summary_text = "\n".join(summary_lines)
    
    # Send or update the message
    if daily_summary_message_id is None:
        # Send new message
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": summary_text}
        try:
            response = requests.post(url, json=payload, timeout=10)
            if response.status_code == 200:
                result = response.json()
                daily_summary_message_id = result["result"]["message_id"]
                logger.debug("📱 Daily summary message sent")
        except Exception as e:
            logger.error(f"Failed to send daily summary: {e}")
    else:
        # Update existing message
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/editMessageText"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "message_id": daily_summary_message_id,
            "text": summary_text
        }
        try:
            requests.post(url, json=payload, timeout=10)
            logger.debug("📱 Daily summary message updated")
        except Exception as e:
            logger.error(f"Failed to update daily summary: {e}")
            # If update fails, reset message_id to send new message next time
            daily_summary_message_id = None

