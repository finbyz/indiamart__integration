# Copyright (c) 2026, Finbyz Tech Pvt Ltd and contributors
# For license information, please see license.txt

import json
import re
from datetime import datetime
from typing import Any

import frappe
from frappe import _
from frappe.integrations.utils import make_get_request
from frappe.model.document import Document
from frappe.utils import (
    add_to_date,
    cint,
    get_datetime,
    now_datetime,
    time_diff_in_seconds,
)

RATE_LIMIT_BUFFER_SECONDS = 20


class IndiamartIntegrationSettings(Document):
    def validate(self):
        self.sync_time = max(cint(self.sync_time or 5), 5)

    @frappe.whitelist()
    def sync_indiamart_leads(
        self,
        start_time: str | None = None,
        end_time: str | None = None,
        create_records: int = 1,
        trigger_source: str = "Manual",
        use_date_only: int | None = None,
    ):
        if not cint(self.get("enable")):
            return {
                "processed_rows": 0,
                "created_customer": 0,
                "created_address": 0,
                "created_lead": 0,
                "status": "Skipped",
                "message": _(
                    "IndiaMart Integration is disabled. Enable it in Indiamart Integration Settings."
                ),
            }

        create_records = int(create_records or 0)
        trigger_source = (trigger_source or "Manual").strip().title()
        fetch_request_type = f"Fetch CRM Leads ({trigger_source})"
        process_request_type = f"Process CRM Lead ({trigger_source})"

        if use_date_only in (None, ""):
            day_wise = cint(self.get("day_wise"))
        else:
            day_wise = cint(use_date_only)

        is_due, due_message = self._is_sync_due(day_wise=day_wise)
        if not is_due:
            self._append_log(
                request_type=fetch_request_type,
                endpoint=(self.get("api_end_point") or "").strip(),
                status="Skipped",
                error_message=due_message,
            )
            self.save(ignore_permissions=True)
            frappe.db.commit()
            return {
                "processed_rows": 0,
                "created_customer": 0,
                "created_address": 0,
                "created_lead": 0,
                "status": "Skipped",
                "message": due_message,
            }

        endpoint = self._get_endpoint()
        api_key = self._get_api_key()
        start_dt, end_dt = self._get_time_window(
            start_time, end_time, day_wise=day_wise
        )

        params: dict = {"glusr_crm_key": api_key}
        if start_dt:
            params["start_time"] = self._format_indiamart_datetime(
                start_dt, date_only=bool(day_wise)
            )
        if end_dt:
            params["end_time"] = self._format_indiamart_datetime(
                end_dt, date_only=bool(day_wise)
            )

        request_payload = self._to_json(self._redact_sensitive_params(params))
        try:
            response = make_get_request(endpoint, params=params)
            is_success, api_message = self._validate_api_response(response)
            if not is_success:
                self._append_log(
                    request_type=fetch_request_type,
                    endpoint=endpoint,
                    request_payload=request_payload,
                    response_payload=self._to_json(response),
                    status="Failed",
                    error_message=api_message,
                )
                self.save(ignore_permissions=True)
                self.db_set("last_sync_on", now_datetime(), update_modified=False)
                frappe.db.commit()
                return {
                    "processed_rows": 0,
                    "created_customer": 0,
                    "created_address": 0,
                    "created_lead": 0,
                    "status": "Failed",
                    "message": api_message,
                }
            rows = self._extract_response_rows(response)
        except Exception:
            self._append_log(
                request_type=fetch_request_type,
                endpoint=endpoint,
                request_payload=request_payload,
                status="Failed",
                error_message=frappe.get_traceback(),
            )
            self.save(ignore_permissions=True)
            frappe.db.commit()
            raise

        self._append_log(
            request_type=fetch_request_type,
            endpoint=endpoint,
            request_payload=request_payload,
            response_payload=self._to_json(response),
            status="Success",
        )

        created_customer = 0
        created_address = 0
        created_lead = 0
        processed = 0

        for row in rows:
            processed += 1
            status = "Success"
            error_message = ""
            result: dict[str, Any] = {}

            try:
                if create_records:
                    customer_name, is_new_customer = self._get_or_create_customer(row)
                    address_name, is_new_address = self._get_or_create_address(
                        row, customer_name
                    )
                    lead_name, is_new_lead = self._get_or_create_lead(
                        row, customer_name
                    )
                    result = {
                        "customer": customer_name,
                        "address": address_name,
                        "lead": lead_name,
                    }
                    created_customer += int(is_new_customer)
                    created_address += int(is_new_address)
                    created_lead += int(is_new_lead)
            except Exception:
                status = "Failed"
                error_message = frappe.get_traceback()

            self._append_log(
                request_type=process_request_type,
                endpoint=endpoint,
                request_payload=self._to_json(row),
                response_payload=self._to_json(result) if result else None,
                status=status,
                error_message=error_message,
            )

        self.save(ignore_permissions=True)
        self.db_set("last_sync_on", now_datetime(), update_modified=False)
        frappe.db.commit()

        return {
            "processed_rows": processed,
            "created_customer": created_customer,
            "created_address": created_address,
            "created_lead": created_lead,
        }

    def _get_api_key(self) -> str:
        api_secret = self.get_password("api_secret", raise_exception=False)
        api_key = (api_secret or self.api_key or "").strip()
        if not api_key:
            frappe.throw(
                _("Set API Key or API Secret in Indiamart Integration Settings")
            )
        return api_key

    def _get_endpoint(self) -> str:
        endpoint = (self.get("api_end_point") or "").strip()
        if not endpoint:
            frappe.throw(_("Set API End Point in Indiamart Integration Settings"))
        return endpoint

    def _get_time_window(
        self, start_time: str | None, end_time: str | None, day_wise: int = 0
    ) -> tuple[datetime | None, datetime | None]:
        max_seconds = 7 * 24 * 60 * 60

        if day_wise:
            # Day-wise mode: use explicit dates or default to today (midnight-to-midnight)
            if start_time:
                start_dt = get_datetime(start_time).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            else:
                start_dt = now_datetime().replace(
                    hour=0, minute=0, second=0, microsecond=0
                )

            if end_time:
                end_dt = get_datetime(end_time).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
            else:
                end_dt = start_dt  # same day — IndiaMart returns full-day data

            if start_dt > end_dt:
                frappe.throw(_("Start Time cannot be greater than End Time"))
            if (end_dt - start_dt).total_seconds() > max_seconds:
                frappe.throw(
                    _("Maximum allowed difference between Start Time and End Time is 7 days")
                )
            return start_dt, end_dt

        else:
            # Minute-wise mode
            if start_time or end_time:
                # Explicit window provided — use it
                end_dt = get_datetime(end_time) if end_time else now_datetime()
                start_dt = (
                    get_datetime(start_time)
                    if start_time
                    else add_to_date(
                        end_dt,
                        minutes=-max(cint(self.sync_time or 5), 5),
                        as_datetime=True,
                    )
                )
                if start_dt > end_dt:
                    frappe.throw(_("Start Time cannot be greater than End Time"))
                if (end_dt - start_dt).total_seconds() > max_seconds:
                    frappe.throw(
                        _("Maximum allowed difference between Start Time and End Time is 7 days")
                    )
                return start_dt, end_dt
            else:
                # No explicit window — omit both so IndiaMart returns last 24 hours
                return None, None

    def _format_indiamart_datetime(
        self, value: datetime, date_only: bool = False
    ) -> str:
        if date_only:
            return value.strftime("%d-%b-%Y")
        return value.strftime("%d-%b-%Y%H:%M:%S")

    def _is_sync_due(self, day_wise: int = 0) -> tuple[bool, str]:
        # Day-wise mode has no rate-limit — each run fetches a full day
        if cint(day_wise):
            return True, ""

        last_sync_on = self.get("last_sync_on")
        if not last_sync_on:
            return True, ""

        interval_minutes = max(cint(self.sync_time or 5), 5)
        elapsed_seconds = time_diff_in_seconds(
            now_datetime(), get_datetime(last_sync_on)
        )
        remaining_seconds = (
            interval_minutes * 60 + RATE_LIMIT_BUFFER_SECONDS
        ) - elapsed_seconds
        if remaining_seconds > 0:
            remaining_mins = int((remaining_seconds + 59) // 60)
            return False, _("Please wait {0} minute(s) before next sync.").format(
                remaining_mins
            )

        return True, ""

    def _extract_response_rows(self, response: Any) -> list[dict]:
        if isinstance(response, list):
            return [row for row in response if isinstance(row, dict)]

        if isinstance(response, dict):
            for key in ("RESPONSE", "response", "DATA", "data", "RESULT", "result"):
                value = response.get(key)
                if isinstance(value, list):
                    return [row for row in value if isinstance(row, dict)]
            if any(
                key in response
                for key in (
                    "UNIQUE_QUERY_ID",
                    "SENDER_EMAIL",
                    "SENDER_MOBILE",
                    "QUERY_MESSAGE",
                )
            ):
                return [response]
            return []

        return []

    def _validate_api_response(self, response: Any) -> tuple[bool, str]:
        if not isinstance(response, dict):
            return True, ""

        status = (
            str(response.get("STATUS") or response.get("status") or "").strip().upper()
        )
        code = str(response.get("CODE") or response.get("code") or "").strip()

        if status and status != "SUCCESS":
            message = (
                response.get("MESSAGE")
                or response.get("message")
                or _("IndiaMART API request failed")
            )
            return False, _("{0} (Status: {1}, Code: {2})").format(
                message, status, code or "N/A"
            )

        return True, ""

    def _append_log(
        self,
        request_type: str,
        endpoint: str,
        request_payload: str | None = None,
        response_payload: str | None = None,
        status: str = "Success",
        error_message: str | None = None,
    ):
        self.append(
            "logs",
            {
                "request_type": request_type,
                "endpoint": endpoint,
                "request_payload": request_payload,
                "response_payload": response_payload,
                "status": status,
                "error_message": error_message,
                "time": now_datetime(),
            },
        )

    def _get_or_create_customer(self, row: dict) -> tuple[str, bool]:
        customer_name = self._get_value(
            row,
            "SENDER_COMPANY",
            "COMPANY",
            "COMPANY_NAME",
            "COMPANYNAME",
            "SENDER_NAME",
            "NAME",
        )
        if not customer_name:
            customer_name = "Indiamart Customer"

        existing = frappe.db.get_value(
            "Customer", {"customer_name": customer_name}, "name"
        )
        if existing:
            return existing, False

        customer_group, territory = self._get_customer_defaults()

        customer = frappe.new_doc("Customer")
        customer.customer_name = customer_name
        customer.customer_type = "Company"
        customer.customer_group = "Default"
        customer.territory = territory
        customer.email_id = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
        customer.mobile_no = self._get_value(
            row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE"
        )
        customer.insert(ignore_permissions=True)

        return customer.name, True

    def _get_or_create_address(self, row: dict, customer: str) -> tuple[str, bool]:
        address_line1 = (
            self._get_value(row, "SENDER_ADDRESS", "ADDRESS", "ADDRESS_LINE1")
            or "Not Available"
        )
        city = self._get_value(row, "SENDER_CITY", "CITY") or "Unknown"
        country = self._get_country(row)

        existing = frappe.db.get_value(
            "Address",
            {
                "address_title": customer,
                "address_line1": address_line1,
                "city": city,
                "country": country,
            },
            "name",
        )
        if existing:
            self._ensure_address_links(existing, customer)
            return existing, False

        address = frappe.new_doc("Address")
        address.address_title = customer
        address.address_type = "Billing"
        address.address_line1 = address_line1
        address.city = city
        address.state = self._get_value(row, "SENDER_STATE", "STATE")
        address.country = country
        address.pincode = self._get_value(row, "SENDER_PINCODE", "PINCODE", "ZIP")
        address.email_id = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
        address.phone = self._get_value(
            row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE"
        )
        address.append("links", {"link_doctype": "Customer", "link_name": customer})
        address.insert(ignore_permissions=True)

        return address.name, True

    def _get_or_create_lead(
        self, row: dict, customer: str | None = None
    ) -> tuple[str, bool]:
        company = self._get_company()
        email_raw = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
        mobile_raw = self._get_value(
            row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE"
        )
        unique_query_id = self._normalize_text(self._get_value(row, "UNIQUE_QUERY_ID"))
        email = self._normalize_email(email_raw)
        mobile = self._normalize_phone(mobile_raw)

        # Deduplicate by UNIQUE_QUERY_ID first — each enquiry is a distinct lead
        if unique_query_id:
            existing = frappe.db.get_value(
                "Lead", {"indiamart_query_id": unique_query_id}, "name"
            )
            if existing:
                return existing, False

        # Fall back to contact + enquiry dedup when query id is absent.
        existing = None
        if email:
            existing = frappe.db.get_value("Lead", {"email_id": email}, "name")
        if not existing and mobile:
            existing = frappe.db.get_value("Lead", {"mobile_no": mobile}, "name")
        if not existing and mobile:
            existing = frappe.db.get_value("Lead", {"phone": mobile}, "name")
        if not existing:
            enquiry_message = self._normalize_text(self._get_value(row, "QUERY_MESSAGE"))
            if enquiry_message and email:
                existing = frappe.db.get_value(
                    "Lead",
                    {"email_id": email, "enquiry_message": enquiry_message},
                    "name",
                )
            if not existing and enquiry_message and mobile:
                existing = frappe.db.get_value(
                    "Lead",
                    {"mobile_no": mobile, "enquiry_message": enquiry_message},
                    "name",
                )
        if existing:
            return existing, False

        lead = frappe.new_doc("Lead")
        full_name = self._get_value(row, "SENDER_NAME", "NAME")
        if full_name:
            lead.first_name = full_name
        else:
            lead.company_name = customer or self._get_value(
                row, "SENDER_COMPANY", "COMPANY"
            )

        lead.company_name = lead.company_name or customer
        lead.email_id = email or email_raw
        lead.mobile_no = mobile or mobile_raw
        lead.phone = self._get_value(row, "PHONE", "SENDER_PHONE")
        lead.city = self._get_value(row, "SENDER_CITY", "CITY")
        lead.state = self._get_value(row, "SENDER_STATE", "STATE")
        lead.country = self._get_value(row, "SENDER_COUNTRY", "COUNTRY") or "India"
        lead.company = company
        lead.request_type = "Product Enquiry"
        lead.source = "Indiamart"

        # IndiaMart-specific custom fields
        lead.indiamart_query_id = unique_query_id
        lead.query_type = self._get_value(row, "QUERY_TYPE")
        lead.product_name = self._get_value(row, "QUERY_PRODUCT_NAME")
        lead.category = self._get_value(row, "QUERY_MCAT_NAME")
        lead.alternate_mobile = self._normalize_phone(
            self._get_value(row, "SENDER_MOBILE_ALT")
        ) or self._get_value(row, "SENDER_MOBILE_ALT")
        lead.enquiry_message = self._get_value(row, "QUERY_MESSAGE")

        lead.insert(ignore_permissions=True)

        return lead.name, True

    def _get_value(self, data: dict, *keys: str) -> str:
        if not isinstance(data, dict):
            return ""

        lower_key_map = {str(k).lower(): v for k, v in data.items()}
        for key in keys:
            value = data.get(key)
            if value is None:
                value = lower_key_map.get(key.lower())
            if value is None:
                continue

            value = str(value).strip()
            if value:
                return value

        return ""

    def _to_json(self, payload: Any) -> str:
        return json.dumps(payload, default=str)

    def _redact_sensitive_params(self, payload: dict[str, Any]) -> dict[str, Any]:
        redacted = dict(payload or {})
        if redacted.get("glusr_crm_key"):
            redacted["glusr_crm_key"] = "***REDACTED***"
        return redacted

    def _normalize_text(self, value: str) -> str:
        return (value or "").strip()

    def _normalize_email(self, value: str) -> str:
        return (value or "").strip().lower()

    def _normalize_phone(self, value: str) -> str:
        digits = re.sub(r"\D+", "", value or "")
        if len(digits) > 10 and digits.startswith("91"):
            digits = digits[-10:]
        return digits

    def _get_customer_defaults(self) -> tuple[str, str]:
        customer_group = frappe.db.get_single_value(
            "Selling Settings", "customer_group"
        ) or frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
        territory = frappe.db.get_single_value(
            "Selling Settings", "territory"
        ) or frappe.db.get_value("Territory", {"is_group": 0}, "name")

        if not customer_group:
            frappe.throw(_("Set default Customer Group in Selling Settings"))
        if not territory:
            frappe.throw(_("Set default Territory in Selling Settings"))

        return customer_group, territory

    def _get_country(self, row: dict) -> str:
        country = self._get_value(row, "SENDER_COUNTRY", "COUNTRY")
        if country:
            return country

        country_iso = self._get_value(row, "SENDER_COUNTRY_ISO", "COUNTRY_ISO")
        if country_iso:
            resolved = frappe.db.get_value(
                "Country", {"code": country_iso.upper()}, "name"
            )
            if resolved:
                return resolved

        return "India"

    def _ensure_address_links(self, address_name: str, customer: str):
        address = frappe.get_doc("Address", address_name)
        existing_links = {(d.link_doctype, d.link_name) for d in address.links}
        has_new_link = False

        if ("Customer", customer) not in existing_links:
            address.append("links", {"link_doctype": "Customer", "link_name": customer})
            has_new_link = True

        if has_new_link:
            address.save(ignore_permissions=True)

    def _get_company(self) -> str:
        company = (self.get("company") or "").strip()
        if company:
            return company

        company = (
            frappe.defaults.get_global_default("company")
            or frappe.db.get_single_value("Global Defaults", "default_company")
            or frappe.db.get_value("Company", {}, "name")
        )
        if not company:
            frappe.throw(_("Set Company in Indiamart Integration Settings"))

        return company


@frappe.whitelist()
def sync_indiamart_leads(
    start_time: str | None = None,
    end_time: str | None = None,
    create_records: int = 1,
    trigger_source: str = "Manual",
    use_date_only: int | None = None,
):
    settings = frappe.get_single("Indiamart Integration Settings")
    return settings.sync_indiamart_leads(
        start_time=start_time,
        end_time=end_time,
        create_records=create_records,
        trigger_source=trigger_source,
        use_date_only=use_date_only,
    )


def scheduled_sync_indiamart_leads():
    settings = frappe.get_single("Indiamart Integration Settings")
    if not cint(settings.get("enable")):
        return
    try:
        settings.sync_indiamart_leads(
            create_records=1,
            trigger_source="Scheduler",
            use_date_only=0,
        )
    except Exception:
        settings._append_log(
            request_type="Fetch CRM Leads (Scheduler)",
            endpoint=(settings.get("api_end_point") or "").strip(),
            status="Failed",
            error_message=frappe.get_traceback(),
        )
        settings.save(ignore_permissions=True)
        frappe.db.commit()
