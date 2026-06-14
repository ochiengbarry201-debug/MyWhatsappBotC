from clinic_twilio import update_twilio_fields, update_template_info
from clinic_setup import onboard_clinic, update_existing_clinic_setup


def mark_subaccount_created(clinic_id: str, subaccount_sid: str, subaccount_auth_token: str):
    update_twilio_fields(clinic_id, {
        "subaccount_sid": subaccount_sid,
        "subaccount_auth_token": subaccount_auth_token,
        "onboarding_status": "subaccount_created"
    })


def mark_waba_connected(clinic_id: str, waba_id: str, business_name: str):
    update_twilio_fields(clinic_id, {
        "waba_id": waba_id,
        "business_name": business_name,
        "onboarding_status": "waba_connected"
    })


def mark_sender_registered(clinic_id: str, whatsapp_sender: str):
    update_twilio_fields(clinic_id, {
        "whatsapp_sender": whatsapp_sender,
        "onboarding_status": "sender_registered"
    })


def mark_template_created(clinic_id: str, template_key: str, friendly_name: str, content_sid: str):
    update_template_info(
        clinic_id,
        template_key=template_key,
        friendly_name=friendly_name,
        content_sid=content_sid,
        status="created"
    )
    update_twilio_fields(clinic_id, {
        "onboarding_status": "template_created"
    })


def mark_template_approved(clinic_id: str, template_key: str):
    update_template_info(
        clinic_id,
        template_key=template_key,
        status="approved"
    )
    update_twilio_fields(clinic_id, {
        "onboarding_status": "template_approved"
    })


def mark_clinic_live(clinic_id: str):
    update_twilio_fields(clinic_id, {
        "onboarding_status": "live"
    })


def onboard_clinic_full(
    clinic_name: str,
    admins,
    spreadsheet_id: str = "",
    sheet_tab: str = "Sheet1",
    timezone: str = "Africa/Nairobi",
    slot_minutes: int = 30,
    weekly: dict = None,

    # routing
    to_number: str = None,

    # twilio metadata
    parent_account_sid: str = "",
    subaccount_sid: str = "",
    subaccount_auth_token: str = "",
    whatsapp_sender: str = "",
    waba_id: str = "",
    business_name: str = "",
    onboarding_status: str = "configured",
    template_language: str = "en",
    templates: dict = None,

    # existing clinic support
    clinic_id: str = None,

    # convenience
    mark_live: bool = False,
):
    """
    One-shot helper:
    - create/update clinic core setup
    - optionally attach routing number
    - save twilio metadata under settings['twilio']
    - optionally save templates
    - optionally mark clinic live
    """
    twilio_payload = {
        "parent_account_sid": parent_account_sid,
        "subaccount_sid": subaccount_sid,
        "subaccount_auth_token": subaccount_auth_token,
        "whatsapp_sender": whatsapp_sender,
        "waba_id": waba_id,
        "business_name": business_name or clinic_name,
        "onboarding_status": onboarding_status or "configured",
        "template_language": template_language or "en",
    }

    if templates and isinstance(templates, dict):
        twilio_payload["templates"] = templates

    if clinic_id:
        result = update_existing_clinic_setup(
            clinic_id=clinic_id,
            clinic_name=clinic_name,
            to_number=to_number,
            admins=admins,
            spreadsheet_id=spreadsheet_id,
            sheet_tab=sheet_tab,
            timezone=timezone,
            slot_minutes=slot_minutes,
            weekly=weekly,
            twilio=twilio_payload,
        )
        final_clinic_id = clinic_id
    else:
        result = onboard_clinic(
            clinic_name=clinic_name,
            to_number=to_number,
            admins=admins,
            spreadsheet_id=spreadsheet_id,
            sheet_tab=sheet_tab,
            timezone=timezone,
            slot_minutes=slot_minutes,
            weekly=weekly,
            twilio=twilio_payload,
        )
        final_clinic_id = result["clinic_id"]

    # Save/merge twilio fields again to guarantee nested twilio section is updated
    update_twilio_fields(final_clinic_id, twilio_payload)

    if templates and isinstance(templates, dict):
        for template_key, template_info in templates.items():
            if not isinstance(template_info, dict):
                continue

            update_template_info(
                final_clinic_id,
                template_key=template_key,
                friendly_name=template_info.get("friendly_name"),
                content_sid=template_info.get("content_sid"),
                status=template_info.get("status"),
            )

    if mark_live:
        mark_clinic_live(final_clinic_id)
        result["live"] = True
    else:
        result["live"] = False

    result["clinic_id"] = final_clinic_id
    result["twilio_fields_saved"] = twilio_payload
    return result