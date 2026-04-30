// Copyright (c) 2026, Finbyz Tech Pvt Ltd and contributors
// For license information, please see license.txt

frappe.ui.form.on("Indiamart Integration Settings", {
	refresh(frm) {

		if (frm.doc.testing) {
			frm.add_custom_button(
				__("Fetch IndiaMART Leads"),
				async () => {
					await run_indiamart_sync(frm, {
						start_time: null,
						end_time: null,
						use_date_only: 0,
						create_records: 1,
						trigger_source: "Manual",
					});
				},
				__("Actions")
			).addClass("btn-primary");
		}

		frm.add_custom_button(
			__("Fetch Leads (Range)"),
			() => {
				open_fetch_range_dialog(frm);
			},
			__("Actions")
		);
	},
});

function open_fetch_range_dialog(frm) {
	const dialog = new frappe.ui.Dialog({
		title: __("Fetch IndiaMART Leads"),
		fields: [
			{
				fieldname: "fetch_mode",
				fieldtype: "Select",
				label: __("Fetch Mode"),
				default: "Current Date",
				options: "Current Date\nBetween Dates",
				description: __("Use Current Date for last-hit/24h sync, or Between Dates for day-wise fetch."),
				reqd: 1,
			},
				{
					fieldname: "start_date",
					fieldtype: "Date",
					label: __("Start Date"),
					depends_on: "eval:doc.fetch_mode==='Between Dates'",
				},
				{
					fieldname: "end_date",
					fieldtype: "Date",
					label: __("End Date"),
					default: frappe.datetime.get_today(),
					depends_on: "eval:doc.fetch_mode==='Between Dates'",
				},
		],
		primary_action_label: __("Fetch"),
		primary_action: async (values) => {
			if (values.fetch_mode === "Between Dates") {
				if (!values.start_date || !values.end_date) {
					frappe.msgprint(__("Start Date and End Date are required."));
					return;
				}

				if (values.start_date > values.end_date) {
					frappe.msgprint(__("Start Date cannot be greater than End Date."));
					return;
				}

				const diff_days = frappe.datetime.get_day_diff(values.end_date, values.start_date);
				if (diff_days > 7) {
					frappe.msgprint(__("Maximum allowed difference between Start and End is 7 days."));
					return;
				}
			}

			await run_indiamart_sync(frm, {
				start_time: values.fetch_mode === "Between Dates" ? values.start_date : null,
				end_time: values.fetch_mode === "Between Dates" ? values.end_date : null,
				use_date_only: values.fetch_mode === "Between Dates" ? 1 : 0,
				create_records: 1,
				trigger_source: "Manual Dialog",
			});
			dialog.hide();
		},
	});

	dialog.show();
}

async function run_indiamart_sync(frm, payload) {
	const response = await frm.call("sync_indiamart_leads", payload);
	const result = response.message || {};
	const api_message = result.message
		? `<br><br>${frappe.utils.escape_html(result.message)}`
		: "";

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
}
