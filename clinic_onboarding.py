from clinic_twilio import update_twilio_fields, update_template_info


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