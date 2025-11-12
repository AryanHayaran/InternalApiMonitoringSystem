from datetime import datetime, timedelta, timezone
from app.utils.connect import db
from app.services.service import ApiService
from app.utils.mail import send_email
from app.utils.loggers import get_logger

logger = get_logger()
api_service = ApiService()

async def send_user_incident_alerts():
    """Periodic task to send grouped incident alerts to users."""
    now = datetime.now(timezone.utc)

    async with db.get_session() as session:
        # 1) Fetch monitored APIs (assumed to return dict-like rows)
        apis = await api_service.get_monitored_apis(session)
        if not apis:
            return

        # user_email -> { user_name, incidents: [...], api_ids: set() }
        user_alerts: dict[str, dict] = {}

        for api in apis:
            period_minutes = api.get("periodic_summary_report") or 0
            if period_minutes <= 0:
                continue  # skip misconfigured rows

            # handle None / naive datetimes
            last_sent = api.get("last_checked_at")
            if last_sent is None:
                # First run: send a summary for the last period window only
                last_sent = now - timedelta(minutes=period_minutes)

            # Normalize to aware UTC
            if last_sent.tzinfo is None:
                last_sent = last_sent.replace(tzinfo=timezone.utc)

            next_send_time = last_sent + timedelta(minutes=period_minutes)
            if now < next_send_time:
                continue  # not time yet

            # Pull incidents since last_sent
            incidents = await api_service.get_incidents_since(session, api["api_id"], last_sent)
            if not incidents:
                # Even if no incidents, advance the window so we don't re-scan the same range forever.
                try:
                    await api_service.update_last_checked(session, api["api_id"], now)
                except Exception as e:
                    logger.exception(f"Failed to advance last_checked for api_id={api['api_id']}: {e}")
                continue

            user_email = api["user_email"]
            bucket = user_alerts.setdefault(user_email, {
                "user_name": api.get("user_name") or "there",
                "incidents": [],
                "api_ids": set(),
            })
            bucket["api_ids"].add(api["api_id"])

            for inc in incidents:
                # Normalize incident times to aware UTC if needed
                st = inc.get("start_time")
                et = inc.get("end_time")
                if st and st.tzinfo is None:
                    st = st.replace(tzinfo=timezone.utc)
                if et and et.tzinfo is None:
                    et = et.replace(tzinfo=timezone.utc)

                bucket["incidents"].append({
                    "api_id": api["api_id"],
                    "start_time": st,
                    "end_time": et,
                    "error": inc.get("initial_error"),
                })

        # 2) Send grouped emails per user; only then advance last_checked for the related APIs
        for user_email, data in user_alerts.items():
            subject = "API Incident Summary Report"
            body_lines = [
                f"Hello {data['user_name']},",
                "",
                "Here are your recent API incidents:",
                "",
            ]
            # Keep body deterministic and readable
            for inc in data["incidents"]:
                body_lines.append(
                    f"- API ID: {inc['api_id']}\n"
                    f"  Start: {inc['start_time']}\n"
                    f"  End:   {inc['end_time']}\n"
                    f"  Error: {inc['error']}\n"
                )
            body_lines.append("Regards,\nHealth Monitor Service")
            body = "\n".join(body_lines)

            try:
                await send_email(to=user_email, subject=subject, body=body)
                logger.info(f"Sent incident report to {user_email}")

                # Advance last_checked for all APIs included in this email
                for api_id in data["api_ids"]:
                    try:
                        await api_service.update_last_checked(session, api_id, now)
                    except Exception as e:
                        logger.exception(f"Failed to update last_checked for api_id={api_id}: {e}")

            except Exception as e:
                # Do NOT advance last_checked if email failed
                logger.exception(f"Failed sending incident report to {user_email}: {e}")
