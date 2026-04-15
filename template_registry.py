TEMPLATE_REGISTRY = {
    "appointment_reminder": {
        "friendly_name": "appointment_reminder",
        "language": "en",
        "variables": ["patient_name", "clinic_name", "date", "time"],
        "body": "Hello {{1}}, this is a reminder from {{2}}. Your appointment is on {{3}} at {{4}}. Reply if you need help."
    }
}