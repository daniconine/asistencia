/** @odoo-module */

import { CalendarRenderer } from '@web/views/calendar/calendar_renderer';

import { DraftTimeOffCalendarCommonRenderer } from './common/calendar_common_renderer';
import { DraftTimeOffCalendarYearRenderer } from './year/calendar_year_renderer';

import { DraftTimeOffDashboard } from '../../dashboard/time_off_dashboard';

export class DraftTimeOffCalendarRenderer extends CalendarRenderer {
    get employeeId() {
        return this.props.model.employeeId;
    }

    get showDashboard() {
        return false;
    }
}

DraftTimeOffCalendarRenderer.template = 'hr_holidays_draft.CalendarRenderer';
DraftTimeOffCalendarRenderer.components = {
    ...DraftTimeOffCalendarRenderer.components,
    day: DraftTimeOffCalendarCommonRenderer,
    week: DraftTimeOffCalendarCommonRenderer,
    month: DraftTimeOffCalendarCommonRenderer,
    year: DraftTimeOffCalendarYearRenderer,
    DraftTimeOffDashboard,
};

export class DraftTimeOffDashboardCalendarRenderer extends DraftTimeOffCalendarRenderer {
    get showDashboard() {
        return !this.env.isSmall;
    }
}
