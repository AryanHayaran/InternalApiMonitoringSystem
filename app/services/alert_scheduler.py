from datetime import datetime, timedelta
from app.db import db
from app.services.service import ApiService
from app.utils.mail import send_email
from app.utils.loggers import get_logger

logger = get_logger()
api_service = ApiService()

async def send_user_incident_alerts():
    """Periodic task to send grouped incident alerts to users."""
    async with db.get_session() as session:
        apis = await api_service.get_monitored_apis(session)

        user_alerts = {}

        for api in apis:
            period_minutes = api["periodic_summary_report"]
            last_sent = api["last_checked_at"]
            next_send_time = last_sent + timedelta(minutes=period_minutes)

            if datetime.utcnow() >= next_send_time:
                incidents = await api_service.get_incidents_since(session, api["api_id"], last_sent)
                if not incidents:
                    continue

                # Group incidents by user
                user_email = api["user_email"]
                if user_email not in user_alerts:
                    user_alerts[user_email] = {
                        "user_name": api["user_name"],
                        "incidents": []
                    }

                for incident in incidents:
                    user_alerts[user_email]["incidents"].append({
                        "api_id": api["api_id"],
                        "start_time": incident["start_time"],
                        "end_time": incident["end_time"],
                        "error": incident["initial_error"]
                    })

                # Update last_sent time after sending
                await api_service.update_last_checked(session, api["api_id"])

        # Send grouped emails
        for user_email, data in user_alerts.items():
            subject = "API Incident Summary Report"
            body_lines = [
                f"Hello {data['user_name']},",
                "\nHere are your recent API incidents:\n"
            ]
            for inc in data["incidents"]:
                body_lines.append(
                    f"- API ID: {inc['api_id']}\n  Start: {inc['start_time']}\n  "
                    f"End: {inc['end_time']}\n  Error: {inc['error']}\n"
                )
            body_lines.append("\nRegards,\nHealth Monitor Service")

            await send_email(to=user_email, subject=subject, body="\n".join(body_lines))
            logger.info(f"Sent incident report to {user_email}")
