/** @odoo-module */

import { calendarView } from '@web/views/calendar/calendar_view';
import { DraftTimeOffCalendarController } from './calendar_controller';
import { DraftTimeOffCalendarModel } from './calendar_model';
import { DraftTimeOffCalendarRenderer, DraftTimeOffDashboardCalendarRenderer } from './calendar_renderer';

import { registry } from '@web/core/registry';

const DraftTimeOffCalendarView = {
    ...calendarView,

    Controller: DraftTimeOffCalendarController,
    Renderer: DraftTimeOffCalendarRenderer,
    Model: DraftTimeOffCalendarModel,

    buttonTemplate: "hr_holidays_draft.CalendarController.controlButtons",
}

registry.category('views').add('draft_time_off_calendar', DraftTimeOffCalendarView);
registry.category('views').add('draft_time_off_calendar_dashboard', {
    ...DraftTimeOffCalendarView,
    Renderer: DraftTimeOffDashboardCalendarRenderer,
});
