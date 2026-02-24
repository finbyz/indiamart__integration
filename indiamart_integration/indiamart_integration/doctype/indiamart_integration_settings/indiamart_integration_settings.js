// Copyright (c) 2026, Finbyz Tech Pvt Ltd and contributors
// For license information, please see license.txt

frappe.ui.form.on("Indiamart Integration Settings", {
	refresh(frm) {
		if (frm.doc.testing){
			frm.add_custom_button(__("Fetch IndiaMART Leads"), async () => {
			const response = await frm.call("sync_indiamart_leads");
			const result = response.message || {};
			const api_message = result.message ? `<br><br>${frappe.utils.escape_html(result.message)}` : "";

			frappe.msgprint(
				__(
					"Processed: {0}<br>Created Customers: {1}<br>Created Addresses: {2}<br>Created Leads: {3}{4}",
					[
						result.processed_rows || 0,
						result.created_customer || 0,
						result.created_address || 0,
						result.created_lead || 0,
						api_message,
					]
				)
			);

			frm.reload_doc();
		}).addClass("btn-primary");	
		}
	},
});
