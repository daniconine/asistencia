/** @odoo-module **/

import { Dialog } from "@web/core/dialog/dialog";

import { CalendarYearPopover } from "@web/views/calendar/calendar_year/calendar_year_popover";

export class DraftTimeOffCalendarYearPopover extends CalendarYearPopover {}
DraftTimeOffCalendarYearPopover.components = { Dialog };
DraftTimeOffCalendarYearPopover.template = "web.CalendarYearPopover";
DraftTimeOffCalendarYearPopover.subTemplates = {
    ...CalendarYearPopover.subTemplates,
    body: "hr_holidays_draft.StressDayCalendarYearPopover.body",
};
