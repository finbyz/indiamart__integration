# # # Copyright (c) 2026, Finbyz Tech Pvt Ltd and contributors
# # # For license information, please see license.txt

# # import json
# # import frappe
# # from frappe import _
# # from typing import Any
# # from datetime import datetime
# # from frappe.integrations.utils import make_get_request
# # from frappe.model.document import Document
# # from frappe.utils import add_to_date, cint, get_datetime, now_datetime, time_diff_in_seconds

# # RATE_LIMIT_BUFFER_SECONDS = 20


# # class IndiamartIntegrationSettings(Document):
# # 	def validate(self):
# # 		self.sync_time = max(cint(self.sync_time or 5), 5)

# # 	@frappe.whitelist()
# # 	def sync_indiamart_leads(
# # 		self,
# # 		start_time: str | None = None,
# # 		end_time: str | None = None,
# # 		create_records: int = 1,
# # 		trigger_source: str = "Manual",
# # 	):
# # 		create_records = int(create_records or 0)
# # 		trigger_source = (trigger_source or "Manual").strip().title()
# # 		fetch_request_type = f"Fetch CRM Leads ({trigger_source})"
# # 		process_request_type = f"Process CRM Lead ({trigger_source})"

# # 		is_due, due_message = self._is_sync_due()
# # 		if not is_due:
# # 			self._append_log(
# # 				request_type=fetch_request_type,
# # 				endpoint=(self.get("api_end_point") or "").strip(),
# # 				status="Skipped",
# # 				error_message=due_message,
# # 			)
# # 			self.save(ignore_permissions=True)
# # 			frappe.db.commit()
# # 			return {
# # 				"processed_rows": 0,
# # 				"created_customer": 0,
# # 				"created_address": 0,
# # 				"created_lead": 0,
# # 				"status": "Skipped",
# # 				"message": due_message,
# # 			}

# # 		endpoint = self._get_endpoint()
# # 		api_key = self._get_api_key()
# # 		start_dt, end_dt = self._get_time_window(start_time, end_time)

# # 		params = {
# # 			"glusr_crm_key": api_key,
# # 			"start_time": self._format_indiamart_datetime(start_dt),
# # 			"end_time": self._format_indiamart_datetime(end_dt),
# # 		}

# # 		request_payload = self._to_json(params)
# # 		try:
# # 			response = make_get_request(endpoint, params=params)
# # 			is_success, api_message = self._validate_api_response(response)
# # 			if not is_success:
# # 				self._append_log(
# # 					request_type=fetch_request_type,
# # 					endpoint=endpoint,
# # 					request_payload=request_payload,
# # 					response_payload=self._to_json(response),
# # 					status="Failed",
# # 					error_message=api_message,
# # 				)
# # 				self.save(ignore_permissions=True)
# # 				self.db_set("last_sync_on", now_datetime(), update_modified=False)
# # 				frappe.db.commit()
# # 				return {
# # 					"processed_rows": 0,
# # 					"created_customer": 0,
# # 					"created_address": 0,
# # 					"created_lead": 0,
# # 					"status": "Failed",
# # 					"message": api_message,
# # 				}
# # 			rows = self._extract_response_rows(response)
# # 		except Exception:
# # 			self._append_log(
# # 				request_type=fetch_request_type,
# # 				endpoint=endpoint,
# # 				request_payload=request_payload,
# # 				status="Failed",
# # 				error_message=frappe.get_traceback(),
# # 			)
# # 			self.save(ignore_permissions=True)
# # 			frappe.db.commit()
# # 			raise

# # 		self._append_log(
# # 			request_type=fetch_request_type,
# # 			endpoint=endpoint,
# # 			request_payload=request_payload,
# # 			response_payload=self._to_json(response),
# # 			status="Success",
# # 		)

# # 		created_customer = 0
# # 		created_address = 0
# # 		created_lead = 0
# # 		processed = 0

# # 		for row in rows:
# # 			processed += 1
# # 			status = "Success"
# # 			error_message = ""
# # 			result: dict[str, Any] = {}

# # 			try:
# # 				if create_records:
# # 					customer_name, is_new_customer = self._get_or_create_customer(row)
# # 					address_name, is_new_address = self._get_or_create_address(row, customer_name)
# # 					lead_name, is_new_lead = self._get_or_create_lead(row, customer_name)
# # 					result = {
# # 						"customer": customer_name,
# # 						"address": address_name,
# # 						"lead": lead_name,
# # 					}
# # 					created_customer += int(is_new_customer)
# # 					created_address += int(is_new_address)
# # 					created_lead += int(is_new_lead)
# # 			except Exception:
# # 				status = "Failed"
# # 				error_message = frappe.get_traceback()

# # 			self._append_log(
# # 				request_type=process_request_type,
# # 				endpoint=endpoint,
# # 				request_payload=self._to_json(row),
# # 				response_payload=self._to_json(result) if result else None,
# # 				status=status,
# # 				error_message=error_message,
# # 			)

# # 		self.save(ignore_permissions=True)
# # 		self.db_set("last_sync_on", now_datetime(), update_modified=False)
# # 		frappe.db.commit()

# # 		return {
# # 			"processed_rows": processed,
# # 			"created_customer": created_customer,
# # 			"created_address": created_address,
# # 			"created_lead": created_lead,
# # 		}

# # 	def _get_api_key(self) -> str:
# # 		api_secret = self.get_password("api_secret", raise_exception=False)
# # 		api_key = (api_secret or self.api_key or "").strip()
# # 		if not api_key:
# # 			frappe.throw(_("Set API Key or API Secret in Indiamart Integration Settings"))
# # 		return api_key

# # 	def _get_endpoint(self) -> str:
# # 		endpoint = (self.get("api_end_point") or "").strip()
# # 		if not endpoint:
# # 			frappe.throw(_("Set API End Point in Indiamart Integration Settings"))
# # 		return endpoint

# # 	def _get_time_window(self, start_time: str | None, end_time: str | None) -> tuple[datetime, datetime]:
# # 		end_dt = get_datetime(end_time) if end_time else now_datetime()
# # 		if start_time:
# # 			start_dt = get_datetime(start_time)
# # 		else:
# # 			minutes = max(cint(self.sync_time or 5), 5)
# # 			start_dt = add_to_date(end_dt, minutes=-minutes, as_datetime=True)

# # 		if start_dt > end_dt:
# # 			frappe.throw(_("Start Time cannot be greater than End Time"))

# # 		return start_dt, end_dt

# # 	def _format_indiamart_datetime(self, value: datetime) -> str:
# # 		return value.strftime("%d-%b-%Y%H:%M:%S")

# # 	def _is_sync_due(self) -> tuple[bool, str]:
# # 		last_sync_on = self.get("last_sync_on")
# # 		if not last_sync_on:
# # 			return True, ""

# # 		interval_minutes = max(cint(self.sync_time or 5), 5)
# # 		elapsed_seconds = time_diff_in_seconds(now_datetime(), get_datetime(last_sync_on))
# # 		remaining_seconds = (interval_minutes * 60 + RATE_LIMIT_BUFFER_SECONDS) - elapsed_seconds
# # 		if remaining_seconds > 0:
# # 			remaining_mins = int((remaining_seconds + 59) // 60)
# # 			return False, _("Please wait {0} minute(s) before next sync.").format(remaining_mins)

# # 		return True, ""

# # 	def _extract_response_rows(self, response: Any) -> list[dict]:
# # 		if isinstance(response, list):
# # 			return [row for row in response if isinstance(row, dict)]

# # 		if isinstance(response, dict):
# # 			for key in ("RESPONSE", "response", "DATA", "data", "RESULT", "result"):
# # 				value = response.get(key)
# # 				if isinstance(value, list):
# # 					return [row for row in value if isinstance(row, dict)]
# # 			return [response]

# # 		return []

# # 	def _validate_api_response(self, response: Any) -> tuple[bool, str]:
# # 		if not isinstance(response, dict):
# # 			return True, ""

# # 		status = str(response.get("STATUS") or response.get("status") or "").strip().upper()
# # 		code = str(response.get("CODE") or response.get("code") or "").strip()

# # 		if status and status != "SUCCESS":
# # 			message = response.get("MESSAGE") or response.get("message") or _("IndiaMART API request failed")
# # 			return False, _("{0} (Status: {1}, Code: {2})").format(message, status, code or "N/A")

# # 		return True, ""

# # 	def _append_log(
# # 		self,
# # 		request_type: str,
# # 		endpoint: str,
# # 		request_payload: str | None = None,
# # 		response_payload: str | None = None,
# # 		status: str = "Success",
# # 		error_message: str | None = None,
# # 	):
# # 		self.append(
# # 			"logs",
# # 			{
# # 				"request_type": request_type,
# # 				"endpoint": endpoint,
# # 				"request_payload": request_payload,
# # 				"response_payload": response_payload,
# # 				"status": status,
# # 				"error_message": error_message,
# # 				"time": now_datetime(),
# # 			},
# # 		)

# # 	def _get_or_create_customer(self, row: dict) -> tuple[str, bool]:
# # 		customer_name = self._get_value(
# # 			row, "SENDER_COMPANY", "COMPANY", "COMPANY_NAME", "COMPANYNAME", "SENDER_NAME", "NAME"
# # 		)
# # 		if not customer_name:
# # 			customer_name = "Indiamart Customer"

# # 		existing = frappe.db.get_value("Customer", {"customer_name": customer_name}, "name")
# # 		if existing:
# # 			return existing, False

# # 		customer_group, territory = self._get_customer_defaults()

# # 		customer = frappe.new_doc("Customer")
# # 		customer.customer_name = customer_name
# # 		customer.customer_type = "Company"
# # 		customer.customer_group = customer_group
# # 		customer.territory = territory
# # 		customer.email_id = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
# # 		customer.mobile_no = self._get_value(row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE")
# # 		customer.insert(ignore_permissions=True)

# # 		return customer.name, True

# # 	def _get_or_create_address(self, row: dict, customer: str) -> tuple[str, bool]:
# # 		company = self._get_company()
# # 		address_line1 = self._get_value(row, "SENDER_ADDRESS", "ADDRESS", "ADDRESS_LINE1") or "Not Available"
# # 		city = self._get_value(row, "SENDER_CITY", "CITY") or "Unknown"
# # 		state = self._get_value(row, "SENDER_STATE", "STATE")
# # 		country = self._get_country(row)

# # 		existing = frappe.db.get_value(
# # 			"Address",
# # 			{
# # 				"address_title": customer,
# # 				"address_line1": address_line1,
# # 				"city": city,
# # 				"country": country,
# # 			},
# # 			"name",
# # 		)
# # 		if existing:
# # 			self._ensure_address_links(existing, customer, company)
# # 			return existing, False

# # 		address = frappe.new_doc("Address")
# # 		address.address_title = customer
# # 		address.address_type = "Billing"
# # 		address.address_line1 = address_line1
# # 		address.city = city
# # 		address.state = state
# # 		address.country = country
# # 		address.pincode = self._get_value(row, "SENDER_PINCODE", "PINCODE", "ZIP")
# # 		address.email_id = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
# # 		address.phone = self._get_value(row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE")
# # 		address.append("links", {"link_doctype": "Customer", "link_name": customer})
# # 		address.append("links", {"link_doctype": "Company", "link_name": company})
# # 		address.insert(ignore_permissions=True)

# # 		return address.name, True

# # 	def _get_or_create_lead(self, row: dict, customer: str | None = None) -> tuple[str, bool]:
# # 		company = self._get_company()
# # 		email = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
# # 		mobile = self._get_value(row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE")

# # 		existing = None
# # 		if email:
# # 			existing = frappe.db.get_value("Lead", {"email_id": email}, "name")
# # 		if not existing and mobile:
# # 			existing = frappe.db.get_value("Lead", {"mobile_no": mobile}, "name")
# # 		if existing:
# # 			return existing, False

# # 		lead = frappe.new_doc("Lead")
# # 		full_name = self._get_value(row, "SENDER_NAME", "NAME")
# # 		if full_name:
# # 			lead.first_name = full_name
# # 		else:
# # 			lead.company_name = customer or self._get_value(row, "SENDER_COMPANY", "COMPANY")

# # 		lead.company_name = lead.company_name or customer
# # 		lead.email_id = email
# # 		lead.mobile_no = mobile
# # 		lead.phone = self._get_value(row, "PHONE", "SENDER_PHONE")
# # 		lead.city = self._get_value(row, "SENDER_CITY", "CITY")
# # 		lead.state = self._get_value(row, "SENDER_STATE", "STATE")
# # 		lead.country = self._get_value(row, "SENDER_COUNTRY", "COUNTRY") or "India"
# # 		lead.company = company
# # 		lead.request_type = "Product Enquiry"
# # 		lead.insert(ignore_permissions=True)

# # 		return lead.name, True

# # 	def _get_value(self, data: dict, *keys: str) -> str:
# # 		if not isinstance(data, dict):
# # 			return ""

# # 		lower_key_map = {str(k).lower(): v for k, v in data.items()}
# # 		for key in keys:
# # 			value = data.get(key)
# # 			if value is None:
# # 				value = lower_key_map.get(key.lower())
# # 			if value is None:
# # 				continue

# # 			value = str(value).strip()
# # 			if value:
# # 				return value

# # 		return ""

# # 	def _to_json(self, payload: Any) -> str:
# # 		return json.dumps(payload, default=str)

# # 	def _get_customer_defaults(self) -> tuple[str, str]:
# # 		customer_group = (
# # 			frappe.db.get_single_value("Selling Settings", "customer_group")
# # 			or frappe.db.get_value("Customer Group", {"is_group": 0}, "name")
# # 		)
# # 		territory = (
# # 			frappe.db.get_single_value("Selling Settings", "territory")
# # 			or frappe.db.get_value("Territory", {"is_group": 0}, "name")
# # 		)

# # 		if not customer_group:
# # 			frappe.throw(_("Set default Customer Group in Selling Settings"))
# # 		if not territory:
# # 			frappe.throw(_("Set default Territory in Selling Settings"))

# # 		return customer_group, territory

# # 	def _get_country(self, row: dict) -> str:
# # 		country = self._get_value(row, "SENDER_COUNTRY", "COUNTRY")
# # 		if country:
# # 			return country

# # 		country_iso = self._get_value(row, "SENDER_COUNTRY_ISO", "COUNTRY_ISO")
# # 		if country_iso:
# # 			resolved = frappe.db.get_value("Country", {"code": country_iso.upper()}, "name")
# # 			if resolved:
# # 				return resolved

# # 		return "India"

# # 	def _ensure_address_links(self, address_name: str, customer: str, company: str):
# # 		address = frappe.get_doc("Address", address_name)
# # 		existing_links = {(d.link_doctype, d.link_name) for d in address.links}

# # 		if ("Customer", customer) not in existing_links:
# # 			address.append("links", {"link_doctype": "Customer", "link_name": customer})
# # 		if ("Company", company) not in existing_links:
# # 			address.append("links", {"link_doctype": "Company", "link_name": company})

# # 		if address.is_dirty():
# # 			address.save(ignore_permissions=True)

# # 	def _get_company(self) -> str:
# # 		company = (self.get("company") or "").strip()
# # 		if company:
# # 			return company

# # 		company = (
# # 			frappe.defaults.get_global_default("company")
# # 			or frappe.db.get_single_value("Global Defaults", "default_company")
# # 			or frappe.db.get_value("Company", {}, "name")
# # 		)
# # 		if not company:
# # 			frappe.throw(_("Set Company in Indiamart Integration Settings"))

# # 		return company


# # @frappe.whitelist()
# # def sync_indiamart_leads(
# # 	start_time: str | None = None,
# # 	end_time: str | None = None,
# # 	create_records: int = 1,
# # 	trigger_source: str = "Manual",
# # ):
# # 	settings = frappe.get_single("Indiamart Integration Settings")
# # 	return settings.sync_indiamart_leads(
# # 		start_time=start_time,
# # 		end_time=end_time,
# # 		create_records=create_records,
# # 		trigger_source=trigger_source,
# # 	)


# # def scheduled_sync_indiamart_leads():
# # 	settings = frappe.get_single("Indiamart Integration Settings")
# # 	try:
# # 		settings.sync_indiamart_leads(create_records=1, trigger_source="Scheduler")
# # 	except Exception:
# # 		settings._append_log(
# # 			request_type="Fetch CRM Leads (Scheduler)",
# # 			endpoint=(settings.get("api_end_point") or "").strip(),
# # 			status="Failed",
# # 			error_message=frappe.get_traceback(),
# # 		)
# # 		settings.save(ignore_permissions=True)
# # 		frappe.db.commit()


# Copyright (c) 2026, Finbyz Tech Pvt Ltd and contributors
# For license information, please see license.txt

import json
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
    ):
        create_records = int(create_records or 0)
        trigger_source = (trigger_source or "Manual").strip().title()
        fetch_request_type = f"Fetch CRM Leads ({trigger_source})"
        process_request_type = f"Process CRM Lead ({trigger_source})"

        is_due, due_message = self._is_sync_due()
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
        start_dt, end_dt = self._get_time_window(start_time, end_time)

        params = {
            "glusr_crm_key": api_key,
            "start_time": self._format_indiamart_datetime(start_dt),
            "end_time": self._format_indiamart_datetime(end_dt),
        }

        request_payload = self._to_json(params)
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
        self, start_time: str | None, end_time: str | None
    ) -> tuple[datetime, datetime]:
        end_dt = get_datetime(end_time) if end_time else now_datetime()
        if start_time:
            start_dt = get_datetime(start_time)
        else:
            minutes = max(cint(self.sync_time or 5), 5)
            start_dt = add_to_date(end_dt, minutes=-minutes, as_datetime=True)

        if start_dt > end_dt:
            frappe.throw(_("Start Time cannot be greater than End Time"))

        return start_dt, end_dt

    def _format_indiamart_datetime(self, value: datetime) -> str:
        return value.strftime("%d-%b-%Y%H:%M:%S")

    def _is_sync_due(self) -> tuple[bool, str]:
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
            return [response]

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
        customer.customer_group = customer_group
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
        email = self._get_value(row, "SENDER_EMAIL", "EMAIL", "EMAIL_ID")
        mobile = self._get_value(row, "SENDER_MOBILE", "MOBILE", "MOBILE_NO", "PHONE")
        unique_query_id = self._get_value(row, "UNIQUE_QUERY_ID")

        # Deduplicate by UNIQUE_QUERY_ID first — each enquiry is a distinct lead
        if unique_query_id:
            existing = frappe.db.get_value(
                "Lead", {"indiamart_query_id": unique_query_id}, "name"
            )
            if existing:
                return existing, False

        # Fall back to email / mobile dedup for backwards compatibility
        existing = None
        if email:
            existing = frappe.db.get_value("Lead", {"email_id": email}, "name")
        if not existing and mobile:
            existing = frappe.db.get_value("Lead", {"mobile_no": mobile}, "name")
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
        lead.email_id = email
        lead.mobile_no = mobile
        lead.phone = self._get_value(row, "PHONE", "SENDER_PHONE")
        lead.city = self._get_value(row, "SENDER_CITY", "CITY")
        lead.state = self._get_value(row, "SENDER_STATE", "STATE")
        lead.country = self._get_value(row, "SENDER_COUNTRY", "COUNTRY") or "India"
        lead.company = company
        lead.request_type = "Product Enquiry"

        # IndiaMart-specific custom fields
        lead.indiamart_query_id = unique_query_id
        lead.query_type = self._get_value(row, "QUERY_TYPE")
        lead.product_name = self._get_value(row, "QUERY_PRODUCT_NAME")
        lead.category = self._get_value(row, "QUERY_MCAT_NAME")
        lead.alternate_mobile = self._get_value(row, "SENDER_MOBILE_ALT")
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

        if ("Customer", customer) not in existing_links:
            address.append("links", {"link_doctype": "Customer", "link_name": customer})

        if address.is_dirty():
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
):
    settings = frappe.get_single("Indiamart Integration Settings")
    return settings.sync_indiamart_leads(
        start_time=start_time,
        end_time=end_time,
        create_records=create_records,
        trigger_source=trigger_source,
    )


def scheduled_sync_indiamart_leads():
    settings = frappe.get_single("Indiamart Integration Settings")
    try:
        settings.sync_indiamart_leads(create_records=1, trigger_source="Scheduler")
    except Exception:
        settings._append_log(
            request_type="Fetch CRM Leads (Scheduler)",
            endpoint=(settings.get("api_end_point") or "").strip(),
            status="Failed",
            error_message=frappe.get_traceback(),
        )
        settings.save(ignore_permissions=True)
        frappe.db.commit()
