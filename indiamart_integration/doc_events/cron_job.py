# indiamart_integration/tasks.py

def scheduled_sync_indiamart_leads():
    from indiamart_integration.indiamart_integration.doctype.indiamart_integration_settings.indiamart_integration_settings import scheduled_sync_indiamart_leads as original

    original()