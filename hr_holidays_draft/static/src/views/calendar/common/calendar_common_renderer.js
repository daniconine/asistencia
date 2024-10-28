/** @odoo-module */

import { CalendarCommonRenderer } from '@web/views/calendar/calendar_common/calendar_common_renderer';

import { useStressDays } from '../../hooks';
import { DraftTimeOffCalendarCommonPopover } from './calendar_common_popover';


export class DraftTimeOffCalendarCommonRenderer extends CalendarCommonRenderer {
    setup() {
        super.setup();
        this.stressDays = useStressDays(this.props);
    }

    onDayRender(info) {
        super.onDayRender(info);
        this.stressDays(info);
    }
}
DraftTimeOffCalendarCommonRenderer.components = {
    ...DraftTimeOffCalendarCommonRenderer,
    Popover: DraftTimeOffCalendarCommonPopover,
}
