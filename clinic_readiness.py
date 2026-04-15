from clinic_twilio import get_twilio_profile


def clinic_can_send_reminders(clinic_id: str):
    profile = get_twilio_profile(clinic_id)

    onboarding_status = profile.get("onboarding_status", "draft")
    sender = profile.get("whatsapp_sender", "").strip()

    template = profile.get("templates", {}).get("appointment_reminder", {})
    content_sid = (template.get("content_sid") or "").strip()
    template_status = template.get("status", "not_created")

    if onboarding_status != "live":
        return False, "blocked_not_live"

    if not sender:
        return False, "blocked_sender_missing"

    if not content_sid:
        return False, "blocked_template_missing"

    if template_status != "approved":
        return False, "blocked_template_not_approved"

    return True, "ok"